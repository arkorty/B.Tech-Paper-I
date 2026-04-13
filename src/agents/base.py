import asyncio
import json
import os
import time
from google import genai
from google.genai import types
from loguru import logger
from engine_guard.core.models import Proposal, NegotiationContext
from typing import Optional

class AgentConfig:
    def __init__(
        self,
        agent_id: str,
        system_goal: str,
        model_name: str = "gemma-3-4b-it",
        draft_models: Optional[list[str]] = None,
        repair_models: Optional[list[str]] = None,
        final_models: Optional[list[str]] = None,
        model_cooldown_seconds: int = 60,
        temperature: float = 0.4,
        allowed_terms: Optional[list[str]] = None,
        hard_constraints: Optional[dict] = None,
        target_goals: Optional[dict[str, float]] = None,
        parallel_drafts: bool = False,
        draft_quality_threshold: float = 0.65,
    ):
        self.agent_id = agent_id
        self.system_goal = system_goal
        self.model_name = model_name
        self.draft_models = self._sanitize_models(draft_models) or [
            "gemini-3.1-flash-lite",
            "gemma-3-27b-it",
        ]
        self.repair_models = self._sanitize_models(repair_models) or [
            "gemini-3-flash",
            "gemma-4-31b",
        ]
        self.final_models = self._sanitize_models(final_models) or ["gemma-4-31b"]
        self.model_cooldown_seconds = max(0, int(model_cooldown_seconds))
        self.temperature = float(temperature)
        self.allowed_terms = allowed_terms or []
        self.hard_constraints = hard_constraints or {}
        self.target_goals = self._sanitize_goals(target_goals)
        self.parallel_drafts = bool(parallel_drafts)
        self.draft_quality_threshold = min(1.0, max(0.0, float(draft_quality_threshold)))

    @staticmethod
    def _sanitize_models(models: Optional[list[str]]) -> list[str]:
        if not models:
            return []

        clean: list[str] = []
        for model in models:
            if not isinstance(model, str):
                continue

            normalized = model.strip()
            if normalized and normalized not in clean:
                clean.append(normalized)

        return clean

    @staticmethod
    def _sanitize_goals(goals: Optional[dict]) -> dict[str, float]:
        if not isinstance(goals, dict):
            return {}

        clean: dict[str, float] = {}
        for key, value in goals.items():
            try:
                clean[str(key)] = float(value)
            except (TypeError, ValueError):
                continue
        return clean

class BaseAgent:
    def __init__(self, config: AgentConfig):
        self.config = config
        api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("Missing GOOGLE_API_KEY (or legacy GEMINI_API_KEY) environment variable.")
        self.client = genai.Client(api_key=api_key)

        self.last_model_used: Optional[str] = None
        self.last_failure_reason: Optional[str] = None
        self.last_cooldown_wait_seconds: float = 0.0
        self._model_cursor = {"draft": 0, "repair": 0, "final": 0}
        self._model_cooldowns: dict[str, float] = {}
        self._selector_lock = asyncio.Lock()

    def _build_prompt(
        self,
        context: NegotiationContext,
        feedback: Optional[str],
        generation_stage: str,
    ) -> str:
        history_text = "No history yet. You are making the first proposal."
        if context.history:
            history_text = "\n".join([
                f"Round {i}: {p.proposing_agent_id} offered { {t.name: t.value for t in p.offered_terms} }. Reasoning: {p.reasoning}. is_final: {p.is_final}"
                for i, p in enumerate(context.history)
            ])

        stage_guidance = ""
        if generation_stage == "final":
            stage_guidance = (
                "You are in final convergence mode. Prioritize stable, realistic terms that can be accepted quickly.\n"
                "If this is your absolute final, walk-away offer, you may set 'is_final' to true.\n"
            )
        elif generation_stage == "repair":
            stage_guidance = (
                "You are in repair mode. Focus on satisfying constraints and avoiding risky values.\n"
            )
        else:
            stage_guidance = (
                "You are in draft mode. DO NOT set 'is_final' to true yet.\n"
            )

        constraints_text = "\nYour Operational Constraints (DO NOT SHARE EXPLICITLY WITH OPPONENT):\n"
        for term in self.config.allowed_terms:
            hard = self.config.hard_constraints.get(term, "Unknown")
            constraints_text += f"- '{term}': Hard limits (Absolute limit before failure) {hard}\n"

        prompt = (
            f"You are a sophisticated AI negotiating agent named {self.config.agent_id}.\n"
            f"Your Goal: {self.config.system_goal}\n"
            f"{constraints_text}\n"
            "Analyze the negotiation history and the task description. Formulate a strategic counter-offer or accept the previous proposal.\n"
            "Do NOT hallucinate resources or parameters outside the scope of negotiation.\n\n"
            f"REQUIRED TERMS: Your 'offered_terms' list MUST contain EXACTLY these term names and no others: {self.config.allowed_terms}\n\n"
            f"{stage_guidance}"
            "Return ONLY valid JSON matching this exact shape:\n"
            "{\n"
            "  \"proposing_agent_id\": \"string\",\n"
            "  \"offered_terms\": [{\"name\": \"string\", \"value\": number}],\n"
            "  \"reasoning\": \"string\",\n"
            "  \"accept_previous\": boolean,\n"
            "  \"is_final\": boolean\n"
            "}\n\n"
            f"Task: {context.task_description}\n"
            f"Negotiation History:\n{history_text}\n\n"
        )

        if feedback:
            prompt += (
                "\n[URGENT: PREVIOUS PROPOSAL REJECTED]\n"
                f"Reason: {feedback}\n"
                "You MUST adjust your limits or targets to resolve this error before proposing again.\n\n"
            )

        prompt += "Action: Create your next proposal."
        return prompt

    @staticmethod
    def _extract_response_text(response) -> str:
        raw_text = (response.text or "").strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:-3]
        elif raw_text.startswith("```"):
            raw_text = raw_text[3:-3]
        return raw_text.strip()

    @staticmethod
    def _to_proposal(raw_text: str) -> Proposal:
        if not raw_text:
            raise ValueError("Empty model response")
        proposal_dict = json.loads(raw_text)
        return Proposal(**proposal_dict)

    def _models_for_stage(self, stage: str) -> list[str]:
        if stage == "repair":
            stage_models = self.config.repair_models
        elif stage == "final":
            stage_models = self.config.final_models
        else:
            stage_models = self.config.draft_models

        if not stage_models:
            stage_models = [self.config.model_name]

        unique: list[str] = []
        for model in stage_models:
            normalized = model.strip()
            if normalized and normalized not in unique:
                unique.append(normalized)
        return unique

    async def _select_model(self, stage: str, skipped: set[str]) -> Optional[str]:
        async with self._selector_lock:
            candidates = self._models_for_stage(stage)
            if not candidates:
                return None

            now = time.monotonic()
            available = [
                model for model in candidates
                if self._model_cooldowns.get(model, 0) <= now and model not in skipped
            ]

            if not available:
                return None

            index = self._model_cursor.get(stage, 0)
            selected = available[index % len(available)]
            self._model_cursor[stage] = index + 1
            return selected

    def _mark_rate_limited(self, model_name: str):
        if self.config.model_cooldown_seconds <= 0:
            return

        self._model_cooldowns[model_name] = (
            time.monotonic() + self.config.model_cooldown_seconds
        )

    def get_stage_cooldown_wait_seconds(self, stage: str) -> float:
        now = time.monotonic()
        waits = []
        for model_name in self._models_for_stage(stage):
            blocked_until = self._model_cooldowns.get(model_name, 0)
            if blocked_until > now:
                waits.append(blocked_until - now)

        if not waits:
            return 0.0

        return max(0.0, min(waits))

    @staticmethod
    def _is_rate_limit_error(error: Exception) -> bool:
        message = str(error).lower()
        return (
            "429" in message
            or "rate" in message
            or "quota" in message
            or "resource_exhausted" in message
            or "too many requests" in message
        )

    async def _generate_with_model(
        self,
        model_name: str,
        prompt: str,
        structured: bool,
    ) -> Proposal:
        if "gemma" in model_name.lower():
            structured = False

        if structured:
            config = types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=Proposal,
                temperature=self.config.temperature,
            )
        else:
            config = types.GenerateContentConfig(temperature=self.config.temperature)

        response = await self.client.aio.models.generate_content(
            model=model_name,
            contents=prompt,
            config=config,
        )

        raw_text = self._extract_response_text(response)
        return self._to_proposal(raw_text)
    
    async def generate_proposal(
        self,
        context: NegotiationContext,
        feedback: Optional[str] = None,
        generation_stage: str = "draft",
    ) -> Optional[Proposal]:
        logger.info(f"[{self.config.agent_id}] Generating proposal for round {context.round_number}...")

        self.last_failure_reason = None
        self.last_cooldown_wait_seconds = 0.0

        attempted_models: set[str] = set()
        stage = generation_stage

        while True:
            model_name = await self._select_model(stage, attempted_models)
            if not model_name:
                break

            logger.info(
                f"[{self.config.agent_id}] Trying model '{model_name}' for stage '{stage}'."
            )

            # Rebuild prompt per attempt so stage/feedback context is always fresh.
            prompt = self._build_prompt(context, feedback, stage)

            try:
                proposal = await self._generate_with_model(
                    model_name=model_name,
                    prompt=prompt,
                    structured=True,
                )
                self.last_model_used = model_name
                return proposal
            except Exception as structured_error:
                logger.warning(
                    f"[{self.config.agent_id}] Structured generation failed with {model_name}: {structured_error}"
                )

                if self._is_rate_limit_error(structured_error):
                    self._mark_rate_limited(model_name)
                    self.last_failure_reason = "cooldown"
                    logger.warning(
                        f"[{self.config.agent_id}] Rate limited on {model_name}. Marked cooldown for {self.config.model_cooldown_seconds}s."
                    )
                    continue

                try:
                    proposal = await self._generate_with_model(
                        model_name=model_name,
                        prompt=prompt,
                        structured=False,
                    )
                    self.last_model_used = model_name
                    return proposal
                except Exception as fallback_error:
                    if self._is_rate_limit_error(fallback_error):
                        self._mark_rate_limited(model_name)
                        self.last_failure_reason = "cooldown"
                        logger.warning(
                            f"[{self.config.agent_id}] Rate limited on {model_name}. Marked cooldown for {self.config.model_cooldown_seconds}s."
                        )
                        continue

                    attempted_models.add(model_name)
                    self.last_failure_reason = "generation_error"
                    logger.error(
                        f"[{self.config.agent_id}] Generation failed with {model_name}: {fallback_error}"
                    )

        self.last_cooldown_wait_seconds = self.get_stage_cooldown_wait_seconds(stage)
        if self.last_cooldown_wait_seconds > 0:
            self.last_failure_reason = "cooldown"
        elif self.last_failure_reason is None:
            self.last_failure_reason = "exhausted"

        return None
