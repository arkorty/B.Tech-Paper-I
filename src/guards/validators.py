import logging
from typing import Dict, Optional, Tuple
from engine_guard.core.models import Proposal

logger = logging.getLogger(__name__)

class ProgrammaticGuard:
    SUPPORTED_MODES = {"full", "bounds-only"}

    def __init__(self, mode: str = "full"):
        """
        Engine Guard to prevent unsafe/hallucinated negotiations.
        """
        normalized_mode = (mode or "full").strip().lower()
        if normalized_mode not in self.SUPPORTED_MODES:
            raise ValueError(
                f"Unsupported guard mode '{mode}'. Supported modes: {sorted(self.SUPPORTED_MODES)}"
            )

        self.mode = normalized_mode
        self.agent_constraints = {}

    def add_agent_constraints(
        self,
        agent_id: str,
        hard_constraints: Dict[str, Tuple[Optional[float], Optional[float]]]
    ):
        self.agent_constraints[agent_id] = {
            "hard": hard_constraints
        }

    def validate_proposal(self, proposal: Proposal) -> Tuple[bool, str]:
        """
        Validates the proposed terms. Returns (is_valid, failure_reason)
        """
        agent_id = proposal.proposing_agent_id
        
        if agent_id not in self.agent_constraints:
            return True, "No constraints defined for this agent."
            
        constraints = self.agent_constraints[agent_id]
        hard_constraints = constraints["hard"]

        terms = {term.name: term.value for term in proposal.offered_terms}

        if self.mode == "full":
            # 0. Missing Terms Check
            missing_terms = [key for key in hard_constraints.keys() if key not in terms]
            unauthorized_terms = [key for key in terms.keys() if key not in hard_constraints]

            if missing_terms or unauthorized_terms:
                error_msgs = []
                if missing_terms:
                    error_msgs.append(f"Missing required negotiation terms: {missing_terms}")
                if unauthorized_terms:
                    error_msgs.append(
                        f"Proposal contains unauthorized terms: {unauthorized_terms}. You must only negotiate on the exact required terms."
                    )

                msg = " AND ".join(error_msgs)
                logger.warning(f"[GUARD FAIL] {msg}")
                return False, msg

        # 1. Hard Constraints Check (Absolute rules - e.g. budget cannot exceed X, time cannot be negative)
        if self.mode == "bounds-only":
            constraint_items = [
                (key, bounds)
                for key, bounds in hard_constraints.items()
                if key in terms
            ]
        else:
            constraint_items = list(hard_constraints.items())

        for key, (min_val, max_val) in constraint_items:
            if key in terms:
                val = terms[key]

                below_min = min_val is not None and val < min_val
                above_max = max_val is not None and val > max_val
                if below_min or above_max:
                    if min_val is None:
                        bounds_text = f"<= {max_val}"
                    elif max_val is None:
                        bounds_text = f">= {min_val}"
                    else:
                        bounds_text = f"[{min_val}, {max_val}]"

                    msg = f"Hard constraint violated for '{key}': {val} not in bounds {bounds_text}"
                    logger.warning(f"[GUARD FAIL] {msg}")
                    return False, msg

        logger.info(
            f"[GUARD PASS] Proposal from {proposal.proposing_agent_id} passed safety checks (mode={self.mode})."
        )
        return True, f"Proposal passed safety constraints in mode '{self.mode}'."
