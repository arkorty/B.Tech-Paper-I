from pydantic import BaseModel, Field
from typing import List

class Term(BaseModel):
    name: str = Field(description="Name of the term (e.g. 'monthly_price')")
    value: float = Field(description="Numerical value of the term")

class Proposal(BaseModel):
    proposing_agent_id: str = Field(description="The ID of the agent making the proposal")
    offered_terms: List[Term] = Field(description="List of numerical terms being negotiated")
    reasoning: str = Field(description="The agent's reasoning behind the proposal")
    accept_previous: bool = Field(description="True if the agent accepts the opponent's previous offer. If true, negotiation ends successfully.")
    is_final: bool = Field(description="True if this is the final, walk-away offer from the agent. If rejected, negotiation fails.")

class NegotiationContext(BaseModel):
    task_description: str
    round_number: int
    history: List[Proposal]
