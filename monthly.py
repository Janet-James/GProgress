
from datetime import datetime
import psycopg2
from django.http import HttpResponse
from fast_bitrix24 import Bitrix

from datetime import datetime, timedelta
#import datetime

# database connection established
conn = psycopg2.connect(database="gprogress", user="postgres", password="postgres", host="localhost", port="5432")
cr = conn.cursor()


def monthly_vise_project():
    try:
        cr.execute("""SELECT start_date,end_date FROM cp_project_monitoring_metrics""")
        project_date = cr.fetchall()
 
        startdate_str = "2023-01-05 03:11:15+05:30"
        enddate_str = "2024-06-09 03:43:55+05:30"
        project_id = 74
     
        startdate = datetime.strptime(startdate_str, "%Y-%m-%d %H:%M:%S%z")
        enddate = datetime.strptime(enddate_str, "%Y-%m-%d %H:%M:%S%z")
        current_date = startdate
        while current_date <= enddate:
            month = int(current_date.strftime("%m"))
            year = current_date.strftime("%Y")   

            print("Month:", month)
            print("Year:", year)
            
            current_date = current_date + timedelta(days=30)  

            # planned_completion
            cr.execute("""SELECT 
                SUM(CASE WHEN solar_project_phase_id = 1 THEN 1 ELSE 0 END) AS phase1_planned_completion_count,
                SUM(CASE WHEN solar_project_phase_id = 2 THEN 1 ELSE 0 END) AS phase2_planned_completion_count,
                SUM(CASE WHEN solar_project_phase_id = 3 THEN 1 ELSE 0 END) AS phase3_planned_completion_count
                FROM 
                cp_project_task_detail
                WHERE 
                group_id = %s 
                AND EXTRACT(MONTH FROM end_date_plan) = %s
                AND EXTRACT(YEAR FROM end_date_plan) = %s
                AND status = %s;""",(project_id,month,year,5))
            planned_completion_date = cr.fetchall() 
            if planned_completion_date:
                print(planned_completion_date)
                phase1_planned_completion_count = planned_completion_date[0][0]
                phase2_planned_completion_count = planned_completion_date[0][1]
                phase3_planned_completion_count = planned_completion_date[0][2]
            else:
                # Handle the case when no data is returned from the query
                phase1_planned_completion_count = 0
                phase2_planned_completion_count = 0
                phase3_planned_completion_count = 0

            print("Phase 1 Completion Count:", phase1_planned_completion_count)
            print("Phase 2 Completion Count:", phase2_planned_completion_count)
            print("Phase 3 Completion Count:", phase3_planned_completion_count)

            month_number = month 
            year_number = year
            group_id = project_id


            cr.execute("""SELECT 
            SUM(CASE WHEN solar_project_phase_id = 1 THEN 1 ELSE 0 END) AS phase1_actual_completion_count,
            SUM(CASE WHEN solar_project_phase_id = 2 THEN 1 ELSE 0 END) AS phase2_actual_completion_count,
            SUM(CASE WHEN solar_project_phase_id = 3 THEN 1 ELSE 0 END) AS phase3_actual_completion_count
            FROM 
                cp_project_task_detail
            WHERE 
            group_id = %s 
            AND EXTRACT(MONTH FROM closed_date) = %s
            AND EXTRACT(YEAR FROM closed_date) = %s
            AND status = %s;""",(project_id,month,year,5))
            actual_completion_date = cr.fetchall()
            print(actual_completion_date)
            if planned_completion_date:
                phase1_actual_completion_count = actual_completion_date[0][0]
                phase2_actual_completion_count=actual_completion_date[0][1]
                phase3_actual_completion_count =actual_completion_date[0][2]
            else:
                phase1_actual_completion_count = 0
                phase2_actual_completion_count = 0
                phase3_actual_completion_count = 0

            month_number =month 
            year_number= year
            group_id = project_id


            cr.execute("""SELECT project_id FROM cp_monthly_wise_project_data WHERE project_id = %s AND month = %s AND year = %s""", (group_id, month_number, year_number))
            project_data_id = cr.fetchone()
            print("S.no: ", project_data_id)

            if project_data_id:
                print("update project")

                cr.execute("""UPDATE cp_monthly_wise_project_data SET phase1_planned_completion_count = %s, phase1_actual_completion_count = %s, phase2_planned_completion_count = %s, phase2_actual_completion_count = %s, phase3_planned_completion_count = %s,
                            phase3_actual_completion_count = %s WHERE project_id = %s AND month = %s AND year = %s RETURNING month""",
                            (phase1_planned_completion_count, phase1_actual_completion_count, phase2_planned_completion_count, phase2_actual_completion_count, phase3_planned_completion_count, phase3_actual_completion_count, group_id, month_number, year_number))
                conn.commit()
                json_data = {'msg': 'Update Successfully', 'status': 2, 'updated_id': group_id}
                print(json_data)

            else:
                print("insert project")

                cr.execute("""INSERT INTO cp_monthly_wise_project_data (project_id, phase1_planned_completion_count, phase1_actual_completion_count, phase2_planned_completion_count, phase2_actual_completion_count, phase3_planned_completion_count,
                            phase3_actual_completion_count, month, year) VALUES 
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING month""",
                            (group_id, phase1_planned_completion_count, phase1_actual_completion_count, phase2_planned_completion_count, phase2_actual_completion_count, phase3_planned_completion_count, phase3_actual_completion_count, month_number, year_number))
                month_inserted = cr.fetchone()[0]
                conn.commit()
                json_data = {'msg': 'Inserted Successfully', 'status': 1, 'created_id': group_id}
                print(json_data)

    except Exception as e:
        print("Error:", e)

monthly_vise_project()


