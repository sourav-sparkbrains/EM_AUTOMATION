from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from langgraph.types import Command

app = FastAPI()


class EMRequest(BaseModel):
    user_id: str
    query: Optional[str] = None
    is_initial: bool = True

    selected_projects: Optional[List[str]] = None
    date_selection: Optional[Dict[str, Any]] = None

    em_details: Optional[List[Dict[str, Any]]] = None
    approval_data: Optional[Dict[str, Any]] = None


class EMResponse(BaseModel):
    status: str
    data: Optional[Dict[str, Any]] = None
    message: Optional[str] = None


@app.post("/process", response_model=EMResponse)
async def process_em_request(request: EMRequest):
    """Single endpoint to handle all EM workflow stages."""

    from core.graph import create_workflow
    from core.state import EMState

    workflow = create_workflow()

    thread_id = request.user_id
    config = {"configurable": {"thread_id": thread_id}}

    try:
        if request.is_initial:
            initial_state = EMState(
                intent=None,
                user_id=request.user_id,
                query=request.query,
                stage=""
            )
            result = workflow.invoke(initial_state, config=config)

        elif request.selected_projects:
            result = workflow.invoke(
                Command(resume=request.selected_projects),
                config=config
            )

        elif request.date_selection:
            result = workflow.invoke(
                Command(resume=request.date_selection),
                config=config
            )
        elif request.em_details:
            result = workflow.invoke(
                Command(resume=request.em_details),
                config=config
            )

        elif request.approval_data:
            result = workflow.invoke(
                Command(resume=request.approval_data),
                config=config
            )

        else:
            raise HTTPException(status_code=400, detail="Invalid request")

        if "__interrupt__" in result:
            interrupt_data = result["__interrupt__"][0].value
            return EMResponse(
                status=interrupt_data["status"],
                data=interrupt_data,
                message=interrupt_data.get("message")
            )

        return EMResponse(
            status="completed",
            data=result,
            message="Workflow completed successfully"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))