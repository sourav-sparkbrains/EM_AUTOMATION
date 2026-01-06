from typing import TypedDict, List, Literal, Optional, Dict, Any
from pydantic import BaseModel, Field

class IntentDetection(BaseModel):
    intent:Literal["check_pending","fill_pending"] = Field(description="Based on the user query, classify the intent into one of the given categories")

class EMState(TypedDict):
    """
        Represents the state of Employee-Management system.
    """
    user_id: str
    query: str
    intent:IntentDetection
    stage:Optional[str]
    pending_dates: Optional[List[str]]
    available_projects: Optional[List[Dict[str, Any]]]
    selected_projects: Optional[List[str]]
    date_selection_mode: Optional[str]
    selected_ranges: Optional[List[Dict[str, Any]]]
    selected_dates: Optional[List[str]]
    form_data: Optional[List[Dict[str, Any]]]
    em_summary: Optional[List[Dict[str, Any]]]
    approval_action: Optional[str]
    validation_passed: Optional[bool]
    sql_queries: Optional[List[str]]
    sql_params: Optional[List[tuple]]
    sql_validation_errors: Optional[List[str]]
    execution_result: Optional[Dict[str, Any]]
    inserted_count: Optional[int]
    final_message: Optional[str]