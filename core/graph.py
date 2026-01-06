from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import InMemorySaver

from core.state import EMState
from core.nodes import (intent_detection_node,fetch_pending_dates_node,fetch_user_projects_node,prepare_date_selection_node,
                        generate_form_for_range_node,generate_summary_node,generate_sql_query_node,validate_sql_query_node,
                        execute_sql_query_node,generate_final_response_node)

checkpointer = InMemorySaver()


#ROUTER FUNCTION
def router_node_after_intent(state: EMState) -> str:

    """
        Routes to the next node based on detected intent.
    """
    intent = state.get("intent").intent

    if intent == "check_pending":
        return "fetch_pending_dates"
    elif intent == "fill_pending":
        return "fetch_user_projects"
    else:
        raise ValueError(f"Unknown intent: {intent}")

#GRAPH CREATION FUNCTION

def create_workflow()-> StateGraph:

    """
        Creates a workflow graph for the Employee-Management system.
    """
    graph = StateGraph(EMState)

    graph.add_node("intent_detection",intent_detection_node)
    graph.add_node("fetch_pending_dates",fetch_pending_dates_node)
    graph.add_node("fetch_user_projects",fetch_user_projects_node)
    graph.add_node("prepare_date_selection",prepare_date_selection_node)
    graph.add_node("generate_form_for_range",generate_form_for_range_node)
    graph.add_node("generate_summary",generate_summary_node)
    graph.add_node("generate_sql_query",generate_sql_query_node)
    graph.add_node("validate_sql_query",validate_sql_query_node)
    graph.add_node("execute_sql_query",execute_sql_query_node)
    graph.add_node("generate_final_response",generate_final_response_node)

    graph.add_edge(START,"intent_detection")
    graph.add_conditional_edges("intent_detection",router_node_after_intent)

    #if intent is check_pending
    graph.add_edge("fetch_pending_dates",END)

    #else if intent is fill_pending
    graph.add_edge("fetch_user_projects","prepare_date_selection")
    graph.add_edge("prepare_date_selection", "generate_form_for_range")
    graph.add_edge("generate_form_for_range", "generate_summary")
    graph.add_edge("generate_summary", "generate_sql_query")
    graph.add_edge("generate_sql_query", "validate_sql_query")
    graph.add_edge("validate_sql_query", "execute_sql_query")
    graph.add_edge("execute_sql_query", "generate_final_response")
    graph.add_edge("generate_sql_query",END)

    app = graph.compile(checkpointer=checkpointer)

    return app

