from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langgraph.types import interrupt, Command
from datetime import datetime, timedelta

from common.db import my_db
from core.state import EMState, IntentDetection
from common.log import logger


def intent_detection_node(state: EMState)-> EMState:
    """
        Node to detect the intent of the user query.
    """
    try:
        logger.info(f"Starting intent detection node for {state["user_id"]}.")
        query = state.get("query", "").lower()

        if not query:
            logger.error("Query is empty.")
            raise ValueError("Query is empty.")

        from common.llm import lama_model

        parser = PydanticOutputParser(pydantic_object=IntentDetection)

        prompt = PromptTemplate(
            template="""You are an intent classification model. Classify the following user query {query} using {format_instructions}""",
            input_variables=['query'],
            partial_variables= {'format_instructions': parser.get_format_instructions()}
        )

        chain = prompt | lama_model | parser

        intent_result = chain.invoke({"query": query})

        state["intent"] = intent_result
        logger.info(f"Detected intent: {intent_result.intent}")
        logger.info(f"Intent detection node completed for {state["user_id"]}.")

        state["stage"] = "intent_detected"
        return state

    except Exception as e:
        logger.error(f"Error in intent detection node for {state["user_id"]}: {str(e)}")
        raise e

def fetch_pending_dates_node(state: EMState) -> EMState:
    """
        Node to fetch pending dates for the user.
    """
    try:
        logger.info(f"Starting fetch pending dates node for {state["user_id"]}.")

        user_id = state.get("user_id", "")

        from common.db import my_db,cursor

        pending_date_query = "select em_date from em_data where user_id = %s and is_em_submitted = %s and is_working_day = %s order by em_date asc"
        cursor.execute(pending_date_query,(user_id,False,True))
        raw_pending_date_results = cursor.fetchall()
        my_db.commit()

        pending_dates = [item['em_date'].strftime("%Y-%m-%d") for item in raw_pending_date_results]
        state["pending_dates"] = pending_dates

        logger.info(f"Fetched {len(pending_dates)} pending dates for user {user_id}.")
        state["stage"] = "fetched_pending_dates"
        return state

    except Exception as e:
        logger.error(f"Error in fetch pending dates node for {state["user_id"]}: {str(e)}")
        raise e

def fetch_user_projects_node(state: EMState) -> dict:
    """
       Node to fetch user projects.
    """
    try:
        logger.info(f"Starting fetch user projects node for {state["user_id"]}.")

        user_id = state.get("user_id", "")

        from common.db import cursor

        fetch_projects_query = "select em_date, project_id, project_name, project_code, client_name from em_data where user_id = %s and is_project_assigned = %s  order by project_name asc"
        cursor.execute(fetch_projects_query,(user_id,True))
        raw_fetch_projects_results = cursor.fetchall()
        my_db.commit()

        state["available_projects"] = raw_fetch_projects_results
        logger.info(f"Fetched {len(raw_fetch_projects_results)} projects for user {user_id}.")

        selected_projects = interrupt({
            "status": "select_projects",
            "available_projects": raw_fetch_projects_results,
            "message": "Please select the projects you want to fill EM for"
        })

        logger.info(f"User selected projects: {selected_projects}")

        return {
            "available_projects": raw_fetch_projects_results,
            "selected_projects": selected_projects,
            "stage": "projects_selected"
        }

    except Exception as e:
        logger.error(f"Error in fetch user projects node for {state["user_id"]}: {str(e)}")
        raise e


def prepare_date_selection_node(state: EMState) -> dict:
    """Node to show pending dates and wait for user date selection."""
    try:
        logger.info(f"Starting date selection node for {state['user_id']}.")

        user_id = state.get("user_id", "")
        selected_projects = state.get("selected_projects", [])

        from common.db import cursor, my_db

        project_placeholders = ','.join(['%s'] * len(selected_projects))
        pending_dates_query = f"select distinct em_date from em_data where user_id = %s and is_em_submitted = %s and is_working_day = %s and project_id in ({project_placeholders}) order by em_date asc"
        params = [user_id, False, True] + selected_projects
        cursor.execute(pending_dates_query, params)
        raw_dates = cursor.fetchall()
        my_db.commit()

        pending_dates = [item['em_date'].strftime("%Y-%m-%d") for item in raw_dates]

        date_selection = interrupt({
            "status": "awaiting_date_selection",
            "pending_dates": pending_dates,
            "selected_projects": selected_projects,
            "message": "Select dates or create date ranges"
        })

        logger.info(f"User date selection: {date_selection}")

        return {
            "pending_dates": pending_dates,
            "date_selection_mode": date_selection.get("date_selection_mode"),
            "selected_ranges": date_selection.get("selected_ranges"),
            "selected_dates": date_selection.get("selected_dates"),
            "stage": "dates_selected"
        }

    except Exception as e:
        logger.error(f"Error in date selection node: {str(e)}")
        raise e


def generate_form_for_range_node(state: EMState) -> EMState:
    """Node to generate form schema for ranges or individual dates."""
    try:
        logger.info(f"Starting generate form for range node for {state['user_id']}.")

        user_id = state.get("user_id", "")
        selected_projects = state.get("selected_projects", [])
        date_selection_mode = state.get("date_selection_mode")

        from common.db import cursor, my_db

        form_data = []

        if date_selection_mode == "ranges":
            selected_ranges = state.get("selected_ranges", [])

            for date_range in selected_ranges:
                range_id = date_range["range_id"]
                start_date = date_range["start_date"]
                end_date = date_range["end_date"]

                for project_id in selected_projects:
                    cursor.execute("""
                                    SELECT user_role, client_name, project_id, project_name, 
                                    task_type, billing_type, upwork_hours, time_spend_hours, 
                                    billable_hours, billable_description, nonbillable_hours, 
                                    nonbillable_description, qa_required, task_incharge_name, 
                                    meter_name, project_code
                                    FROM em_data
                                    WHERE user_id = %s AND project_id = %s
                                    LIMIT 1
                                """, (user_id, project_id))

                    project_data = cursor.fetchone()
                    cursor.fetchall()
                    if project_data:
                        form_data.append({
                            "range_id": range_id,
                            "start_date": start_date,
                            "end_date": end_date,
                            **project_data
                        })
                my_db.commit()

        else:
            selected_dates = state.get("selected_dates", [])
            if not isinstance(selected_dates, list):
                selected_dates = [selected_dates]

            for date in selected_dates:
                for project_id in selected_projects:
                    cursor.execute("""
                                    SELECT user_role, client_name, project_id, project_name, 
                                    task_type, billing_type, upwork_hours, time_spend_hours, 
                                    billable_hours, billable_description, nonbillable_hours, 
                                    nonbillable_description, qa_required, task_incharge_name, 
                                    meter_name, project_code
                                    FROM em_data
                                    WHERE user_id = %s AND project_id = %s
                                    LIMIT 1
                                        """, (user_id, project_id))

                    project_data = cursor.fetchone()
                    cursor.fetchall()
                    if project_data:
                        form_data.append({
                            "date": date,
                            **project_data
                        })
                my_db.commit()

        logger.info(f"Generated form data with {len(form_data)} entries")

        state["form_data"] = form_data
        state["stage"] = "form_ready"
        return state

    except Exception as e:
        logger.error(f"Error in generate form for range node for {state['user_id']}: {str(e)}")
        raise e


def generate_summary_node(state: EMState) -> dict:
    """Show form to user, collect all EM entries, then show summary."""

    try:
        from common.db import cursor, my_db

        logger.info(f"Starting generate summary node for {state['user_id']}.")

        user_id = state.get("user_id", "")
        form_data = state.get("form_data", [])
        date_selection_mode = state.get("date_selection_mode")

        em_details = interrupt({
            "status": "collect_all_em_details",
            "form_data": form_data,
            "date_selection_mode": date_selection_mode,
            "message": "Fill EM details for all selected ranges/dates"
        })

        logger.info(f"Received {len(em_details)} EM entries from user")

        expanded_entries = []

        for entry in em_details:
            cursor.execute("""
                SELECT project_name, project_code, client_name 
                FROM em_data 
                WHERE user_id = %s AND project_id = %s 
                LIMIT 1
            """, (user_id, entry["project_id"]))
            project_info = cursor.fetchone()
            cursor.fetchall()
            my_db.commit()

            if date_selection_mode == "ranges" and "start_date" in entry and "end_date" in entry:
                start = datetime.strptime(entry["start_date"], "%Y-%m-%d")
                end = datetime.strptime(entry["end_date"], "%Y-%m-%d")

                current = start
                while current <= end:
                    expanded_entries.append({
                        "date": current.strftime("%Y-%m-%d"),
                        "project_id": entry["project_id"],
                        "project_name": project_info["project_name"] if project_info else "",
                        "project_code": project_info["project_code"] if project_info else "",
                        "client_name": project_info["client_name"] if project_info else "",
                        "hours": entry.get("hours", 0),
                        "task_type": entry.get("task_type", ""),
                        "description": entry.get("description", ""),
                        "billing_type": entry.get("billing_type", "Hourly"),
                        "upwork_hours": entry.get("upwork_hours", 0),
                        "time_spend_hours": entry.get("time_spend_hours", entry.get("hours", 0)),
                        "billable_hours": entry.get("billable_hours", entry.get("hours", 0)),
                        "billable_description": entry.get("billable_description", ""),
                        "nonbillable_hours": entry.get("nonbillable_hours", 0),
                        "nonbillable_description": entry.get("nonbillable_description", ""),
                        "qa_required": entry.get("qa_required", False),
                        "task_incharge_name": entry.get("task_incharge_name", ""),
                        "meter_name": entry.get("meter_name", "")
                    })
                    current += timedelta(days=1)
            else:
                expanded_entries.append({
                    "date": entry.get("date", ""),
                    "project_id": entry["project_id"],
                    "project_name": project_info["project_name"] if project_info else "",
                    "project_code": project_info["project_code"] if project_info else "",
                    "client_name": project_info["client_name"] if project_info else "",
                    "hours": entry.get("hours", 0),
                    "task_type": entry.get("task_type", ""),
                    "description": entry.get("description", ""),
                    "billing_type": entry.get("billing_type", "Hourly"),
                    "upwork_hours": entry.get("upwork_hours", 0),
                    "time_spend_hours": entry.get("time_spend_hours", entry.get("hours", 0)),
                    "billable_hours": entry.get("billable_hours", entry.get("hours", 0)),
                    "billable_description": entry.get("billable_description", ""),
                    "nonbillable_hours": entry.get("nonbillable_hours", 0),
                    "nonbillable_description": entry.get("nonbillable_description", ""),
                    "qa_required": entry.get("qa_required", False),
                    "task_incharge_name": entry.get("task_incharge_name", ""),
                    "meter_name": entry.get("meter_name", "")
                })

        validation_errors = []
        entries_by_date = {}

        for entry in expanded_entries:
            date = entry["date"]
            if date not in entries_by_date:
                entries_by_date[date] = []
            entries_by_date[date].append(entry)

        for date, entries in entries_by_date.items():
            total_hours = sum(e["hours"] for e in entries)
            if total_hours != 8:
                validation_errors.append(f"Total hours for {date} exceed 24 hours ({total_hours}h)")

            for entry in entries:
                if entry["hours"] > 8:
                    validation_errors.append(f"Hours for {entry['project_name']} on {date} cannot exceed 8 hours")

            project_ids = [e["project_id"] for e in entries]
            if len(project_ids) != len(set(project_ids)):
                validation_errors.append(f"Duplicate project entries found for {date}")

        validation_passed = len(validation_errors) == 0

        logger.info(
            f"Expanded to {len(expanded_entries)} entries. Validation: {'Passed' if validation_passed else 'Failed'}")

        approval_response = interrupt({
            "status": "awaiting_approval",
            "em_summary": expanded_entries,
            "total_entries": len(expanded_entries),
            "validation_passed": validation_passed,
            "validation_errors": validation_errors if not validation_passed else [],
            "message": "Review and approve EM entries" if validation_passed else "Please fix validation errors"
        })

        logger.info(f"User approval action: {approval_response}")

        state["em_summary"] = approval_response.get("em_summary", expanded_entries)
        state["approval_action"] = approval_response.get("action")
        state["validation_passed"] = validation_passed
        state["stage"] = "summary_generated"

        return {
            "em_summary": approval_response.get("em_summary", expanded_entries),
            "approval_action": approval_response.get("action"),
            "validation_passed": validation_passed,
            "stage": "approved"
        }

    except Exception as e:
        logger.error(f"Error in generate summary node for {state['user_id']}: {str(e)}")
        raise e


def generate_sql_query_node(state: EMState) -> EMState:
    """Generate parameterized SQL queries for EM insertion."""
    try:
        logger.info(f"Starting SQL query generation for {state['user_id']}.")

        em_summary = state.get("em_summary", [])
        user_id = state.get("user_id", "")

        sql_queries = []
        sql_params = []

        insert_query = """
            UPDATE em_data 
            SET 
                is_em_submitted = %s,
                task_type = %s,
                time_spend_hours = %s,
                time_spend_minutes = %s,
                billable_hours = %s,
                billable_minutes = %s,
                billable_description = %s,
                nonbillable_hours = %s,
                nonbillable_minutes = %s,
                nonbillable_description = %s,
                qa_required = %s,
                task_incharge_name = %s,
                meter_name = %s,
                billing_type = %s,
                upwork_hours = %s,
                updated_at = NOW()
            WHERE user_id = %s 
            AND em_date = %s 
            AND project_id = %s
            AND is_em_submitted = %s
        """

        for entry in em_summary:
            params = (
                True,
                entry.get("task_type", "Development"),
                entry.get("time_spend_hours", 0),
                0,
                entry.get("billable_hours", 0),
                0,
                entry.get("billable_description", ""),
                entry.get("nonbillable_hours", 0),
                0,
                entry.get("nonbillable_description", ""),
                entry.get("qa_required", False),
                entry.get("task_incharge_name", ""),
                entry.get("meter_name", ""),
                entry.get("billing_type", "Hourly"),
                entry.get("upwork_hours", 0),
                user_id,
                entry.get("date"),
                entry.get("project_id"),
                False
            )

            sql_queries.append(insert_query)
            sql_params.append(params)

        logger.info(f"Generated {len(sql_queries)} parameterized SQL queries")

        state["sql_queries"] = sql_queries
        state["sql_params"] = sql_params
        state["stage"] = "sql_generated"
        return state

    except Exception as e:
        logger.error(f"Error generating SQL queries: {str(e)}")
        raise e


def validate_sql_query_node(state: EMState) -> EMState:
    """Validate SQL queries for security and business rules."""
    try:
        logger.info(f"Starting SQL validation for {state['user_id']}.")

        sql_queries = state.get("sql_queries", [])
        sql_params = state.get("sql_params", [])
        em_summary = state.get("em_summary", [])

        validation_errors = []

        dangerous_keywords = [
            'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'EXEC',
            'EXECUTE', 'UNION', '--', '/*', '*/', 'xp_', 'sp_'
        ]

        for query in sql_queries:
            query_upper = query.upper()
            for keyword in dangerous_keywords:
                if keyword in query_upper and keyword not in ['UPDATE', 'INSERT']:
                    validation_errors.append(f"Dangerous SQL keyword detected: {keyword}")

        for idx, params in enumerate(sql_params):
            entry = em_summary[idx]

            time_spend_hours = params[2]
            if not isinstance(time_spend_hours, (int, float)) or time_spend_hours < 0 or time_spend_hours > 8:
                validation_errors.append(f"Invalid hours for entry {idx}: {time_spend_hours}")

            date_str = params[16]
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                validation_errors.append(f"Invalid date format: {date_str}")

            valid_task_types = ['Development', 'Design', 'HR', 'QA', 'Testing', 'Meeting', 'Review', 'Other']
            task_type = params[1]
            if task_type not in valid_task_types:
                validation_errors.append(f"Invalid task type: {task_type}")

        from common.db import cursor, my_db
        user_id = state.get("user_id", "")

        for idx, entry in enumerate(em_summary):
            project_id = entry.get("project_id")
            em_date = entry.get("date")

            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM em_data 
                WHERE user_id = %s AND project_id = %s AND is_project_assigned = %s
            """, (user_id, project_id, True))
            result = cursor.fetchone()
            cursor.fetchall()

            if result['count'] == 0:
                validation_errors.append(f"Project {project_id} not assigned to user")

            if datetime.strptime(em_date, "%Y-%m-%d") > datetime.now():
                validation_errors.append(f"Cannot submit EM for future date: {em_date}")

            cursor.execute("""
                SELECT is_em_submitted 
                FROM em_data 
                WHERE user_id = %s AND em_date = %s AND project_id = %s
            """, (user_id, em_date, project_id))
            existing = cursor.fetchone()
            cursor.fetchall()

            if existing and existing['is_em_submitted']:
                validation_errors.append(f"EM already submitted for {em_date}, {project_id}")

        my_db.commit()

        validation_passed = len(validation_errors) == 0

        logger.info(f"Validation {'passed' if validation_passed else 'failed'}")
        if not validation_passed:
            logger.warning(f"Validation errors: {validation_errors}")

        state["sql_validation_errors"] = validation_errors
        state["validation_passed"] = validation_passed
        state["stage"] = "sql_validated"
        return state

    except Exception as e:
        logger.error(f"Error validating SQL: {str(e)}")
        raise e


def execute_sql_query_node(state: EMState) -> EMState:
    """Execute SQL queries in a transaction."""
    try:
        logger.info(f"Starting SQL execution for {state['user_id']}.")

        validation_passed = state.get("validation_passed", False)

        if not validation_passed:
            logger.error("Validation failed. Skipping execution.")
            state["execution_result"] = {
                "success": False,
                "message": "Validation failed",
                "errors": state.get("sql_validation_errors", [])
            }
            state["stage"] = "execution_failed"
            return state

        sql_queries = state.get("sql_queries", [])
        sql_params = state.get("sql_params", [])

        from common.db import cursor, my_db

        inserted_count = 0

        try:
            my_db.start_transaction()

            for idx, (query, params) in enumerate(zip(sql_queries, sql_params)):
                cursor.execute(query, params)
                if cursor.rowcount > 0:
                    inserted_count += 1
                logger.info(f"Executed query {idx + 1}/{len(sql_queries)}")

            my_db.commit()

            logger.info(f"Successfully inserted/updated {inserted_count} EM entries")

            state["execution_result"] = {
                "success": True,
                "message": f"Successfully submitted {inserted_count} EM entries",
                "inserted_count": inserted_count
            }
            state["inserted_count"] = inserted_count
            state["stage"] = "execution_completed"

        except Exception as e:
            my_db.rollback()
            logger.error(f"Transaction failed, rolled back: {str(e)}")

            state["execution_result"] = {
                "success": False,
                "message": f"Database error: {str(e)}",
                "inserted_count": 0
            }
            state["stage"] = "execution_failed"

        return state

    except Exception as e:
        logger.error(f"Error executing SQL: {str(e)}")
        raise e


def generate_final_response_node(state: EMState) -> EMState:
    """Generate final response message."""
    try:
        logger.info(f"Generating final response for {state['user_id']}.")

        execution_result = state.get("execution_result", {})
        success = execution_result.get("success", False)

        if success:
            inserted_count = state.get("inserted_count", 0)
            message = f"Success! {inserted_count} EM entries submitted successfully."

            state["final_message"] = message
            state["stage"] = "completed"
        else:
            errors = execution_result.get("message", "Unknown error")
            message = f"Failed to submit EM entries. Error: {errors}"

            state["final_message"] = message
            state["stage"] = "failed"

        logger.info(f"Final response: {message}")
        return state

    except Exception as e:
        logger.error(f"Error generating final response: {str(e)}")
        raise e
