import pandas as pd
import os
from mysql.connector import Error

def create_table(cursor):
    """Create the em_data table"""
    print("\nCreating table 'em_data'...")

    create_table_query = """
    CREATE TABLE IF NOT EXISTS em_data (
        em_id INT AUTO_INCREMENT PRIMARY KEY,

        user_id VARCHAR(50) NOT NULL,
        user_name VARCHAR(100) NOT NULL,
        user_email VARCHAR(100) NOT NULL,
        user_role VARCHAR(50) NOT NULL,

        em_date DATE NOT NULL,
        is_em_submitted BOOLEAN DEFAULT FALSE,

        client_id VARCHAR(50) NOT NULL,
        client_name VARCHAR(100) NOT NULL,
        project_id VARCHAR(50) NOT NULL,
        project_name VARCHAR(200) NOT NULL,
        project_code VARCHAR(50) NOT NULL,
        is_project_assigned BOOLEAN DEFAULT TRUE,

        task_for VARCHAR(20) DEFAULT 'Self',
        task_type VARCHAR(50) NOT NULL,

        billing_type VARCHAR(20) DEFAULT 'Hourly',
        upwork_hours INT DEFAULT 0,
        upwork_minutes INT DEFAULT 0,
        time_spend_hours INT DEFAULT 0,
        time_spend_minutes INT DEFAULT 0,
        billable_hours INT DEFAULT 0,
        billable_minutes INT DEFAULT 0,
        billable_description TEXT,
        nonbillable_hours INT DEFAULT 0,
        nonbillable_minutes INT DEFAULT 0,
        nonbillable_description TEXT,

        qa_required BOOLEAN DEFAULT FALSE,
        qa_approved BOOLEAN DEFAULT FALSE,
        task_incharge_id VARCHAR(50),
        task_incharge_name VARCHAR(100),
        meter_id VARCHAR(50),
        meter_name VARCHAR(100),

        is_working_day BOOLEAN DEFAULT TRUE,
        is_holiday BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        INDEX idx_user_date (user_id, em_date),
        INDEX idx_date (em_date),
        INDEX idx_user_submitted (user_id, is_em_submitted)
    )
    """

    try:
        cursor.execute(create_table_query)
        print("Table 'em_data' created successfully")
    except Error as e:
        print(f"Error creating table: {e}")


def import_excel_data(connection, cursor, excel_file_path):
    """Import data from CSV file"""
    print(f"\nReading Excel file: {excel_file_path}")

    try:
        df = pd.read_excel(excel_file_path)
        print(f"Read {len(df)} rows from Excel")

        df = df.where(pd.notna(df), None)

        insert_query = """
        INSERT INTO em_data (
            user_id, user_name, user_email, user_role,
            em_date, is_em_submitted,
            client_id, client_name, project_id, project_name, project_code, is_project_assigned,
            task_for, task_type,
            billing_type, upwork_hours, upwork_minutes,
            time_spend_hours, time_spend_minutes,
            billable_hours, billable_minutes, billable_description,
            nonbillable_hours, nonbillable_minutes, nonbillable_description,
            qa_required, qa_approved,
            task_incharge_id, task_incharge_name, meter_id, meter_name,
            is_working_day, is_holiday
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        print("Inserting data into database...")
        inserted_count = 0

        for index, row in df.iterrows():
            try:
                cursor.execute(insert_query, tuple(row))
                inserted_count += 1

                if (index + 1) % 10 == 0:
                    print(f"   Inserted {index + 1} rows...")

            except Error as e:
                print(f"Error inserting row {index + 1}: {e}")
                continue

        connection.commit()
        print(f"Successfully inserted {inserted_count} rows")

        return inserted_count

    except FileNotFoundError:
        print(f"Excel file not found: {excel_file_path}")
        return 0
    except Exception as e:
        print(f"Error importing Excel: {e}")
        return 0


def show_summary(cursor):
    """Show database summary"""
    print("\n" + "=" * 60)
    print("DATABASE SUMMARY")
    print("=" * 60)

    try:
        cursor.execute("SELECT COUNT(*) as total FROM em_data")
        total = cursor.fetchone()['total']
        cursor.fetchall()
        print(f"Total Records: {total}")

        cursor.execute("SELECT COUNT(DISTINCT user_id) as users FROM em_data")
        users = cursor.fetchone()['users']
        cursor.fetchall()
        print(f"Total Users: {users}")

        cursor.execute("SELECT COUNT(*) as submitted FROM em_data WHERE is_em_submitted = TRUE")
        submitted = cursor.fetchone()['submitted']
        cursor.fetchall()
        cursor.execute("SELECT COUNT(*) as pending FROM em_data WHERE is_em_submitted = FALSE")
        pending = cursor.fetchone()['pending']
        cursor.fetchall()
        print(f"Submitted EMs: {submitted}")
        print(f"Pending EMs: {pending}")

        print("\nPENDING DATES BY USER:")
        print("-" * 60)

        cursor.execute("""
            SELECT user_id, user_name, COUNT(*) as pending_count
            FROM em_data
            WHERE is_em_submitted = FALSE
            GROUP BY user_id, user_name
            ORDER BY pending_count DESC
        """)

        for row in cursor.fetchall():
            print(f"   {row['user_name']} ({row['user_id']}): {row['pending_count']} pending days")

        print("\nSample Pending Dates for USR001:")
        print("-" * 60)

        cursor.execute("""
            SELECT em_date, project_name
            FROM em_data
            WHERE user_id = 'USR001'
            AND is_em_submitted = FALSE
            ORDER BY em_date ASC
            LIMIT 5
        """)

        for row in cursor.fetchall():
            print(f"   • {row['em_date']} - {row['project_name']}")

    except Error as e:
        print(f"Error fetching summary: {e}")


def main():
    """Main function"""
    from common.db import create_connection

    connection = create_connection()

    if connection is None:
        print("Failed to connect to database. Exiting...")
        return

    try:
        cursor = connection.cursor(dictionary=True)

        create_table(cursor)

        excel_file =os.getenv("EXCEL_PATH")
        inserted = import_excel_data(connection, cursor, excel_file)

        if inserted > 0:
            show_summary(cursor)

        print("\n" + "=" * 60)
        print("IMPORT COMPLETED SUCCESSFULLY!")
        print("=" * 60)

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("\nDatabase connection closed")


if __name__ == "__main__":
    main()