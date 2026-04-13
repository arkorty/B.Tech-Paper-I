import asyncio
from loguru import logger
from engine_guard.core.models import NegotiationContext, Proposal
from engine_guard.agents.base import BaseAgent
from engine_guard.guards.validators import ProgrammaticGuard
from typing import Callable, Awaitable

class NegotiationEngine:
    def __init__(
        self, 
        agent_a: BaseAgent, 
        agent_b: BaseAgent, 
        guard: ProgrammaticGuard | None,
        task_description: str,
        max_rounds_a: int = 10,
        max_rounds_b: int = 10,
        max_retries_per_turn: int = 2
    ):
        self.agents = [agent_a, agent_b]
        self.agent_max_rounds = {
            agent_a.config.agent_id: max_rounds_a,
            agent_b.config.agent_id: max_rounds_b
        }
        self.total_turn_budget = max_rounds_a + max_rounds_b
        self.guard = guard
        self.context = NegotiationContext(
            task_description=task_description,
            round_number=0,
            history=[]
        )
        self.max_retries = max_retries_per_turn
        self.max_engine_backoff_seconds = 60.0

    @staticmethod
    def _is_high_rpm_draft_model(model_name: str) -> bool:
        normalized = (model_name or "").strip().lower()
        return "gemma-3" in normalized

    def _parallel_attempts_for_stage(self, agent: BaseAgent, generation_stage: str) -> int:
        if generation_stage != "draft":
            return 1

        if not agent.config.parallel_drafts:
            return 1

        draft_models = agent.config.draft_models or [agent.config.model_name]
        primary_draft_model = draft_models[0] if draft_models else ""
        if not self._is_high_rpm_draft_model(primary_draft_model):
            return 1

        return 2

    @staticmethod
    def _score_draft_quality(agent: BaseAgent, proposal: Proposal) -> tuple[float, dict[str, float]]:
        if not agent.config.target_goals:
            return 1.0, {}

        offered_terms = {term.name: term.value for term in proposal.offered_terms}
        normalized_distances: dict[str, float] = {}

        for term_name, target_value in agent.config.target_goals.items():
            if term_name not in offered_terms:
                continue

            proposed_value = offered_terms[term_name]
            min_val, max_val = agent.config.hard_constraints.get(term_name, (None, None))

            if min_val is not None and max_val is not None and max_val > min_val:
                span = max_val - min_val
            elif min_val is not None:
                span = max(abs(target_value - min_val), 1.0)
            elif max_val is not None:
                span = max(abs(max_val - target_value), 1.0)
            else:
                span = max(abs(target_value), 1.0)

            distance = abs(proposed_value - target_value) / max(span, 1e-6)
            normalized_distances[term_name] = min(distance, 1.0)

        if not normalized_distances:
            return 1.0, {}

        avg_distance = sum(normalized_distances.values()) / len(normalized_distances)
        score = max(0.0, 1.0 - avg_distance)
        return score, normalized_distances

    @staticmethod
    def _build_quality_feedback(
        agent: BaseAgent,
        proposal: Proposal,
        score: float,
        threshold: float,
        normalized_distances: dict[str, float],
    ) -> str:
        offered_terms = {term.name: term.value for term in proposal.offered_terms}
        sorted_terms = sorted(
            normalized_distances.items(),
            key=lambda item: item[1],
            reverse=True,
        )
        top_terms = sorted_terms[:2]

        adjustment_hints = []
        for term_name, _ in top_terms:
            adjustment_hints.append(
                f"{term_name}: target {agent.config.target_goals[term_name]}, current {offered_terms.get(term_name)}"
            )

        hints = "; ".join(adjustment_hints)
        return (
            f"Draft quality score {score:.2f} is below threshold {threshold:.2f}. "
            f"Move terms closer to target goals ({hints})."
        )

    def _pick_generation_stage(
        self,
        current_agent_id: str,
        agent_turn_counts: dict[str, int],
        feedback: str | None,
    ) -> str:
        # Guard feedback always takes precedence over normal staging.
        if feedback:
            return "repair"

        # If the opponent just issued a walk-away offer, force final-response behavior.
        if self.context.history and self.context.history[-1].is_final:
            return "final"

        current_budget = self.agent_max_rounds[current_agent_id]
        current_used = agent_turn_counts[current_agent_id]
        current_remaining = max(0, current_budget - current_used)

        total_used = sum(agent_turn_counts.values())
        total_budget = max(1, self.total_turn_budget)
        progress_ratio = total_used / total_budget

        # Enter final mode in the last third of an agent's own budget,
        # or once overall negotiation progress crosses 60%.
        if current_remaining <= max(2, current_budget // 3):
            return "final"
        if progress_ratio >= 0.60:
            return "final"

        return "draft"
        
    async def run_negotiation(self, on_event: Callable[[dict], Awaitable[None]] = None) -> str:
        logger.info("Starting Task-Agnostic Negotiation.")
        logger.info(f"Task: {self.context.task_description}")
        if self.guard is None:
            logger.warning("Programmatic guard disabled. Bypassing safety validation.")
        
        async def emit(evt_type, data):
            if on_event:
                await on_event({"type": evt_type, **data})

        await emit("ENGINE_START", {"task_description": self.context.task_description})
        
        turn_index = 0
        last_accepted_node_id = "root"
        
        agent_turn_counts = {
            self.agents[0].config.agent_id: 0,
            self.agents[1].config.agent_id: 0
        }
        
        while True:
            current_agent = self.agents[turn_index % 2]
            current_agent_id = current_agent.config.agent_id
            
            if agent_turn_counts[current_agent_id] >= self.agent_max_rounds[current_agent_id]:
                msg = f"FAIL - Timeout after {agent_turn_counts[current_agent_id]} turns for {current_agent_id}."
                logger.warning(msg)
                await emit(
                    "NEGOTIATION_END",
                    {
                        "status": msg,
                        "agreement_reached": False,
                        "accepted_offer": None,
                    },
                )
                return msg
            
            valid_proposal = None
            feedback_message = None
            accepted_offer_node_id = None
            cooldown_backoff_seconds = 1.0
            
            await emit("TURN_START", {
                "round_number": self.context.round_number,
                "agent_id": current_agent.config.agent_id
            })

            async def attempt_generation(
                attempt_index: int,
                fb: str | None,
                generation_stage: str,
            ):
                attempt_node_id = f"R{self.context.round_number}_A{attempt_index}"
                parent_node_id = last_accepted_node_id
                logger.info(
                    f"[{current_agent_id}] Round {self.context.round_number} attempt {attempt_index + 1} using stage '{generation_stage}'."
                )
                
                await emit("PROPOSAL_GENERATING", {
                    "node_id": attempt_node_id,
                    "parent_id": parent_node_id,
                    "agent_id": current_agent.config.agent_id,
                    "attempt": attempt_index + 1,
                    "generation_stage": generation_stage,
                })
                
                proposal = await current_agent.generate_proposal(
                    self.context,
                    feedback=fb,
                    generation_stage=generation_stage,
                )
                
                if not proposal:
                    if current_agent.last_failure_reason == "cooldown":
                        cooldown_wait = max(
                            current_agent.last_cooldown_wait_seconds,
                            current_agent.get_stage_cooldown_wait_seconds(generation_stage),
                        )
                        logger.warning(
                            f"[{current_agent_id}] All {generation_stage} models are cooling down. Earliest retry in {cooldown_wait:.1f}s."
                        )
                        return None, attempt_node_id, parent_node_id, False, None, cooldown_wait

                    logger.error("Failed to generate a proposal with available models.")
                    return (
                        None,
                        attempt_node_id,
                        parent_node_id,
                        False,
                        "Failed to generate a proposal with the current model pool.",
                        0.0,
                    )
                
                # Prevent Agent Identity Spoofing
                proposal.proposing_agent_id = current_agent_id
                
                # Prevent Trojan Horse Acceptance Exploit
                if proposal.accept_previous and len(self.context.history) > 0:
                    proposal.offered_terms = self.context.history[-1].offered_terms
                
                # Prevent early walk-away in draft mode
                if generation_stage == "draft":
                    proposal.is_final = False
                
                # Check for Duplicate Term Exploits before creating dictionary
                if proposal.offered_terms:
                    names = [t.name for t in proposal.offered_terms]
                    if len(names) != len(set(names)):
                        return (
                            None,
                            attempt_node_id,
                            parent_node_id,
                            False,
                            "Duplicate terms provided in proposal.",
                            0.0,
                        )

                terms_dict = {t.name: t.value for t in proposal.offered_terms} if proposal.offered_terms else {}
                
                await emit("PROPOSAL_GENERATED", {
                    "node_id": attempt_node_id,
                    "agent_id": current_agent.config.agent_id,
                    "model_name": current_agent.last_model_used,
                    "proposal": {
                        "terms": terms_dict,
                        "reasoning": proposal.reasoning,
                        "accept_previous": proposal.accept_previous,
                        "is_final": proposal.is_final
                    }
                })

                await asyncio.sleep(1) # delay for visualization
                if self.guard is None:
                    is_valid = True
                    current_feedback = "Programmatic guard disabled; proposal validation bypassed."
                    await emit("GUARD_EVALUATION", {
                        "node_id": attempt_node_id,
                        "passed": True,
                        "details": current_feedback,
                        "bypassed": True,
                    })
                else:
                    is_valid, current_feedback = self.guard.validate_proposal(proposal)
                    
                    if is_valid:
                        await emit("GUARD_EVALUATION", {
                            "node_id": attempt_node_id,
                            "passed": True,
                            "details": "Proposal passed strict safety constraints."
                        })
                    else:
                        await emit("GUARD_EVALUATION", {
                            "node_id": attempt_node_id,
                            "passed": False,
                            "details": current_feedback
                        })

                if is_valid and generation_stage == "draft" and current_agent.config.target_goals:
                    quality_score, distances = self._score_draft_quality(current_agent, proposal)
                    quality_threshold = current_agent.config.draft_quality_threshold

                    if quality_score < quality_threshold:
                        quality_feedback = self._build_quality_feedback(
                            current_agent,
                            proposal,
                            quality_score,
                            quality_threshold,
                            distances,
                        )
                        await emit(
                            "LOCAL_QUALITY_EVALUATION",
                            {
                                "node_id": attempt_node_id,
                                "passed": False,
                                "score": round(quality_score, 3),
                                "threshold": round(quality_threshold, 3),
                                "details": quality_feedback,
                            },
                        )
                        return (
                            None,
                            attempt_node_id,
                            parent_node_id,
                            False,
                            quality_feedback,
                            0.0,
                        )

                    await emit(
                        "LOCAL_QUALITY_EVALUATION",
                        {
                            "node_id": attempt_node_id,
                            "passed": True,
                            "score": round(quality_score, 3),
                            "threshold": round(quality_threshold, 3),
                            "details": "Draft proposal is close enough to configured target goals.",
                        },
                    )
                    
                return proposal, attempt_node_id, parent_node_id, is_valid, current_feedback, 0.0

            attempt_count = 0
            while not valid_proposal and attempt_count < self.max_retries:
                generation_stage = self._pick_generation_stage(
                    current_agent_id=current_agent_id,
                    agent_turn_counts=agent_turn_counts,
                    feedback=feedback_message,
                )

                planned_parallelism = self._parallel_attempts_for_stage(current_agent, generation_stage)
                parallelism = min(planned_parallelism, self.max_retries - attempt_count)

                if parallelism > 1:
                    tasks = [
                        attempt_generation(
                            attempt_index=attempt_count + i,
                            fb=feedback_message,
                            generation_stage=generation_stage,
                        )
                        for i in range(parallelism)
                    ]
                    results = await asyncio.gather(*tasks)
                else:
                    results = [
                        await attempt_generation(
                            attempt_index=attempt_count,
                            fb=feedback_message,
                            generation_stage=generation_stage,
                        )
                    ]

                attempt_count += parallelism
                
                cooldown_waits: list[float] = []
                latest_feedback = feedback_message

                for proposal, attempt_node_id, parent_node_id, is_valid, fb, cooldown_wait in results:
                    if is_valid and proposal:
                        valid_proposal = proposal
                        last_accepted_node_id = attempt_node_id
                        accepted_offer_node_id = parent_node_id
                        break
                    
                    if cooldown_wait > 0:
                        cooldown_waits.append(cooldown_wait)
                    elif fb:
                        latest_feedback = fb

                feedback_message = latest_feedback

                if valid_proposal:
                    break

                if cooldown_waits:
                    retry_after = min(cooldown_waits)
                    sleep_for = min(
                        self.max_engine_backoff_seconds,
                        max(cooldown_backoff_seconds, retry_after),
                    )
                    await emit(
                        "ENGINE_BACKOFF",
                        {
                            "agent_id": current_agent_id,
                            "generation_stage": generation_stage,
                            "retry_in_seconds": round(sleep_for, 2),
                        },
                    )
                    logger.warning(
                        f"[{current_agent_id}] Model pool cooling down for stage '{generation_stage}'. Backing off for {sleep_for:.1f}s."
                    )
                    await asyncio.sleep(sleep_for)
                    cooldown_backoff_seconds = min(
                        self.max_engine_backoff_seconds,
                        cooldown_backoff_seconds * 2,
                    )
                else:
                    cooldown_backoff_seconds = 1.0
            
            if not valid_proposal:
                msg = f"FAIL - {current_agent.config.agent_id} could not produce a safe proposal."
                logger.error(msg)
                await emit(
                    "NEGOTIATION_END",
                    {
                        "status": msg,
                        "agreement_reached": False,
                        "accepted_offer": None,
                    },
                )
                return msg
                
            self.context.history.append(valid_proposal)
            self.context.round_number += 1
            
            if valid_proposal.accept_previous and turn_index > 0:
                msg = f"SUCCESS - {valid_proposal.proposing_agent_id} accepted the terms."
                logger.success("Agreement reached!")
                accepted_offer = self.context.history[-2] if len(self.context.history) >= 2 else None
                accepted_terms = {
                    t.name: t.value for t in accepted_offer.offered_terms
                } if accepted_offer and accepted_offer.offered_terms else {}

                await emit(
                    "NEGOTIATION_END",
                    {
                        "status": msg,
                        "agreement_reached": True,
                        "winning_node_id": accepted_offer_node_id or last_accepted_node_id,
                        "accepted_offer": {
                            "node_id": accepted_offer_node_id or last_accepted_node_id,
                            "offered_by_agent_id": accepted_offer.proposing_agent_id if accepted_offer else "Unknown Agent",
                            "accepted_by_agent_id": valid_proposal.proposing_agent_id,
                            "terms": accepted_terms,
                            "reasoning": accepted_offer.reasoning if accepted_offer else "",
                        },
                    },
                )
                return msg
                
            if len(self.context.history) >= 2 and self.context.history[-2].is_final:
                msg = f"FAIL - {current_agent.config.agent_id} rejected a final walk-away offer from {self.context.history[-2].proposing_agent_id}."
                logger.warning(msg)
                await emit(
                    "NEGOTIATION_END",
                    {
                        "status": msg,
                        "agreement_reached": False,
                        "accepted_offer": None,
                    },
                )
                return msg
                
            if valid_proposal.is_final:
                logger.info(f"{valid_proposal.proposing_agent_id} submitted a final walk-away offer.")
                
            agent_turn_counts[current_agent_id] += 1
            turn_index += 1
