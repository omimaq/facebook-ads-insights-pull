import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import json
import requests
import os
import time
import csv

default_dag_args = {
    "owner": "User01",
    ## Trigger for next day
    "depends_on_past": False,
    ## Setting start date as yesterday starts the DAG immediately when it is # detected in the Cloud Storage bucket.
    "start_date": datetime(2023, 2, 16, 0, 30),
    # To email on failure or retry set 'email' arg to your email and enable # emailing here.
    "email": "abc@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    # If a task fails, retry it once after waiting at least 15 minutes
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "project_id": "project",
}

with DAG(
    dag_id="fb_ads_data",
    description="DAG to get daily FB ads insights of active ad accounts",
    schedule_interval="45 0 * * *",
    default_args=default_dag_args,
) as dag:
    # Dummy Variable For Start
    START = DummyOperator(task_id="START")
    # Dummy Variable For End
    FINISH = DummyOperator(task_id="FINISH")

    # Function to send single request instead of batch
    def send_single_insights_request(
        ad_account_id, fields, access_token, day_before_yesterday
    ):
        # Send on D-2 level
        url = (
            f"https://graph.facebook.com/v15.0/act_{ad_account_id}/insights?"
            f"level=ad&locale=en_US&fields={fields}&time_increment=1&"
            f"time_range={{'since':'{day_before_yesterday}','until':'{day_before_yesterday}'}}"
            f"&access_token={access_token}"
        )
        response_insights = requests.request("POST", url)
        response_insights_json = response_insights.json()
        report_run_id = response_insights_json["report_run_id"]
        return report_run_id

    # Get the status of each asynchronous job
    def get_status(report_run_id, access_token):
        url_status = f"https://graph.facebook.com/v15.0/{report_run_id}?access_token={access_token}"
        response_status = requests.get(url_status)
        account_id = json.loads(response_status.text)["account_id"]
        async_status = json.loads(response_status.text)["async_status"]
        date_start = json.loads(response_status.text)["date_start"]
        date_stop = json.loads(response_status.text)["date_stop"]
        return account_id, async_status, date_start, date_stop

    def get_facebook_insights():
        # Check for any csv files and delete them
        directory_path = "/home/airflow/gcs/data"

        for filename in os.listdir(directory_path):
            if filename.endswith(".csv"):
                file_path = os.path.join(directory_path, filename)
                os.remove(file_path)
                print(f"{file_path} has been deleted.")

        # Get ad account IDs
        scopes = [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
        # Get credentials from json file
        credentials = service_account.Credentials.from_service_account_file(
            "/home/airflow/gcs/dags/project-bcdeee60c0e1.json", scopes=scopes
        )
        project = "project"

        # Initiate a new client for BQ
        client = bigquery.Client(credentials=credentials, project=project)

        query = """
            SELECT Ad_Account_ID
            FROM `project.project_facebookads.project_facebookads_accounts`
            WHERE Active = true
        """

        # Execute the query and store the result to a list
        query_job = client.query(query)
        ad_account_id_list = [row.Ad_Account_ID for row in query_job]

        # Define fields
        fields = [
            "account_id,account_name,ad_id,ad_name,adset_id,adset_name,buying_type,campaign_id,campaign_name,clicks,conversions,cost_per_action_type,cost_per_conversion,cost_per_thruplay,cost_per_unique_click,cpc,cpm,cpp,ctr,date_start,date_stop,engagement_rate_ranking,frequency,full_view_impressions,full_view_reach,impressions,objective,optimization_goal,outbound_clicks,outbound_clicks_ctr,place_page_name,quality_ranking,reach,social_spend,spend,video_30_sec_watched_actions,video_play_actions,website_ctr"
        ]

        # Get access token
        with open(
            "/home/airflow/gcs/dags/facebook_insights_api_access_token.json", "r"
        ) as file:
            token = json.load(file)

        access_token = token["access_token"]

        # Making batch request based on the configs
        batch_size = 40  # Maximum size is 50 according to FB documentation but I don't believe it

        # Split the list into 40 items each chunk and send it in batch mode
        for i in range(0, len(ad_account_id_list), batch_size):
            batch_request = []
            status_list = []
            # IDEA: List to store status but no delete when completed. Send this list to table in BQ to store the status in case of logging.
            batch_ad_account_id_list = ad_account_id_list[i : i + batch_size]
            try:
                # Define D-2 date
                day_before_yesterday = datetime.today().date() - timedelta(days=2)
                for ad_account_id in batch_ad_account_id_list:
                    batch_request.append(
                        {
                            "method": "POST",
                            "relative_url": f"v15.0/act_{ad_account_id}/insights?level=ad&locale=en_US&fields={fields}&time_increment=1&time_range={{'since':'{day_before_yesterday}','until':'{day_before_yesterday}'}}&access_token={access_token}",
                        }
                    )

                # Execute batch request
                batch_response_insights = requests.post(
                    "https://graph.facebook.com/v15.0",
                    params={"access_token": access_token},
                    headers={"Content-Type": "application/json"},
                    data=json.dumps({"batch": batch_request}),
                ).json()

                now = datetime.now()

                # Iterate through responses and get status about the job
                for response_insights in batch_response_insights:
                    headers = response_insights["headers"]
                    for header in headers:
                        if header["name"] == "X-FB-Ads-Insights-Throttle":
                            x_fb_ads_insights_throttle = json.loads(header["value"])
                            app_id_util_pct = x_fb_ads_insights_throttle[
                                "app_id_util_pct"
                            ]
                            acc_id_util_pct = x_fb_ads_insights_throttle[
                                "acc_id_util_pct"
                            ]
                            # Print the final limit of the accounts
                            # print(f"acc_id_util_pct:{acc_id_util_pct}, app_id_util_pct:{app_id_util_pct}")
                            break

                    # Check for limits
                    if app_id_util_pct > 90 or acc_id_util_pct > 90:
                        print(f"Limit reached at report_run_id:{report_run_id}")
                        raise ValueError("Limit reached!")
                    else:
                        body = json.loads(response_insights["body"])
                        report_run_id = body["report_run_id"]
                        # Add request info to list
                        status_list.append(
                            {
                                "report_run_id": report_run_id,
                                "run_time": now,
                                "status": "Initiated",
                                "date_start": "date_start",
                                "date_stop": "date_stop",
                                "run_count": 1,
                            }
                        )

                while status_list:
                    for i, report_status in enumerate(status_list):
                        # Get the report_run_id
                        report_run_id = report_status["report_run_id"]
                        run_time = report_status["run_time"]
                        run_count = report_status["run_count"]
                        # Get status for each job
                        account_id, async_status, date_start, date_stop = get_status(
                            report_run_id, access_token
                        )

                        report_status["date_start"] = date_start
                        report_status["date_stop"] = date_stop
                        report_status["status"] = async_status

                        now = datetime.now()
                        run_interval = now - run_time

                        if (
                            async_status
                            in ["Job Not Started", "Job Started", "Job Running"]
                            and run_count >= 1
                            and run_interval < timedelta(minutes=2)
                        ):
                            # Add 1 run_count
                            report_status["run_count"] = run_count + 1
                            print(
                                f"Job running, report_run_id:{report_run_id}, run_count:{run_count}, run_interval:{run_interval}"
                            )
                            # Wait for 2 seconds
                            time.sleep(2)
                        elif (
                            async_status == "Job Running"
                            and run_count > 1
                            and run_interval > timedelta(minutes=2)
                        ):
                            # Send the job again
                            report_run_id = send_single_insights_request(
                                account_id, fields, access_token, day_before_yesterday
                            )
                            # Get status for the job
                            async_status, date_start, date_stop = get_status(
                                report_run_id, access_token
                            )
                            # Add request info to list
                            now = datetime.now()
                            status_list.append(
                                {
                                    "report_run_id": report_run_id,
                                    "run_time": now,
                                    "status": async_status,
                                    "date_start": date_start,
                                    "date_stop": date_stop,
                                    "run_count": 1,
                                }
                            )

                            print(
                                f"Job exceed 2 mins limit, send the request again for the same time range: ad_account_id:{account_id}, report_run_id:{report_run_id}, date_start:{date_start}, date_stop:{date_stop}"
                            )

                            # Delete the record status
                            del status_list[i]
                            # Wait for 2 seconds
                            time.sleep(2)
                        elif async_status == "Job Failed":
                            # Send the job again
                            report_run_id = send_single_insights_request(
                                account_id, fields, access_token, day_before_yesterday
                            )
                            # Get status for the job
                            async_status, date_start, date_stop = get_status(
                                report_run_id, access_token
                            )
                            # Add request info to list
                            now = datetime.now()
                            status_list.append(
                                {
                                    "report_run_id": report_run_id,
                                    "run_time": now,
                                    "status": async_status,
                                    "date_start": date_start,
                                    "date_stop": date_stop,
                                    "run_count": 1,
                                }
                            )

                            print(
                                f"Job failed, send the request again for the same time range: ad_account_id:{account_id}, report_run_id:{report_run_id}, date_start:{date_start}, date_stop:{date_stop}"
                            )

                            # Delete the record status
                            del status_list[i]
                            # Wait for 2 seconds
                            time.sleep(2)
                        elif async_status == "Job Skipped":
                            # Send the job again
                            report_run_id = send_single_insights_request(
                                account_id, fields, access_token, day_before_yesterday
                            )
                            # Get status for the job
                            async_status, date_start, date_stop = get_status(
                                report_run_id, access_token
                            )
                            # Add request info to list
                            now = datetime.now()
                            status_list.append(
                                {
                                    "report_run_id": report_run_id,
                                    "run_time": now,
                                    "status": async_status,
                                    "date_start": date_start,
                                    "date_stop": date_stop,
                                    "run_count": 1,
                                }
                            )

                            print(
                                f"Job skipped, send the request again for the same time range: ad_account_id:{account_id}, report_run_id:{report_run_id}, date_start:{date_start}, date_stop:{date_stop}"
                            )

                            # Delete the record status
                            del status_list[i]
                            # Wait for 2 seconds
                            time.sleep(2)
                        elif async_status == "Job Completed":
                            # Name the export csv files
                            csv_file_raw = os.path.join(
                                directory_path,
                                f"{account_id}_{date_start}_{date_stop}_raw.csv",
                            )
                            csv_file_ordered = os.path.join(
                                directory_path,
                                f"{account_id}_{date_start}_{date_stop}_ordered.csv",
                            )

                            # Export report
                            url_export = f"https://www.facebook.com/ads/ads_insights/export_report?report_run_id={report_run_id}&format=csv&level=ad&locale=en_US&time_increment=1&data_columns={fields}&access_token={access_token}"

                            response_csv = requests.request("GET", url_export)
                            search_string = "No data available."
                            # Check if a match was found
                            if search_string in response_csv.text:
                                print(
                                    f"No data for ad_account_id:{account_id}, report_run_id:{report_run_id} from {date_start} to {date_stop}"
                                )
                            else:
                                with open(csv_file_raw, "w") as f:
                                    f.write(response_csv.text)

                                new_order = [
                                    "Reporting starts",
                                    "Reporting ends",
                                    "Cost per ThruPlay (USD)",
                                    "CompleteFlow",
                                    "Website CTR",
                                    "Account ID",
                                    "Account name",
                                    "Ad ID",
                                    "Ad name",
                                    "Ad set ID",
                                    "Ad Set Name",
                                    "Buying type",
                                    "Campaign ID",
                                    "Campaign name",
                                    "Clicks (all)",
                                    "Cost per unique click (all) (USD)",
                                    "CPC (All) (USD)",
                                    "CPM (cost per 1,000 impressions) (USD)",
                                    "Cost per 1,000 Accounts Center accounts reached (USD)",
                                    "CTR (all)",
                                    "Engagement rate ranking",
                                    "Frequency",
                                    "Impressions",
                                    "Objective",
                                    "Optimization goal",
                                    "Business Name",
                                    "Quality ranking",
                                    "Reach",
                                    "Social spend (USD)",
                                    "Amount spent (USD)",
                                    "Outbound clicks",
                                    "Outbound CTR (click-through rate)",
                                    "CTR (link click-through rate)",
                                    "Video plays",
                                    "ClickButton",
                                ]

                                # Read the input file and reorder the fields
                                with open(csv_file_raw, "r") as input_file, open(
                                    csv_file_ordered, "w", newline=""
                                ) as output_file:
                                    reader = csv.DictReader(input_file)
                                    writer = csv.DictWriter(
                                        output_file, fieldnames=new_order
                                    )

                                    writer.writeheader()
                                    for row in reader:
                                        new_row = {}
                                        for field in new_order:
                                            if field in row:
                                                new_row[field] = row[field]
                                            else:
                                                new_row[field] = ""
                                        writer.writerow(new_row)

                                # Send to BQ
                                # Upload csv file to BQ
                                table_id = "project.project_facebookads.project_facebookads_insights_di"

                                job_config = bigquery.LoadJobConfig(
                                    schema=[
                                        bigquery.SchemaField("date_start", "DATE"),
                                        bigquery.SchemaField("date_stop", "DATE"),
                                        bigquery.SchemaField(
                                            "cost_per_thruplay", "FLOAT"
                                        ),
                                        bigquery.SchemaField(
                                            "video_play_actions", "INTEGER"
                                        ),
                                        bigquery.SchemaField("website_ctr", "STRING"),
                                        bigquery.SchemaField("account_id", "INTEGER"),
                                        bigquery.SchemaField("account_name", "STRING"),
                                        bigquery.SchemaField("ad_id", "INTEGER"),
                                        bigquery.SchemaField("ad_name", "STRING"),
                                        bigquery.SchemaField("adset_id", "INTEGER"),
                                        bigquery.SchemaField("adset_name", "STRING"),
                                        bigquery.SchemaField("buying_type", "STRING"),
                                        bigquery.SchemaField("campaign_id", "STRING"),
                                        bigquery.SchemaField("campaign_name", "STRING"),
                                        bigquery.SchemaField("clicks", "INTEGER"),
                                        bigquery.SchemaField(
                                            "cost_per_unique_click", "FLOAT"
                                        ),
                                        bigquery.SchemaField("cpc", "FLOAT"),
                                        bigquery.SchemaField("cpm", "FLOAT"),
                                        bigquery.SchemaField("cpp", "FLOAT"),
                                        bigquery.SchemaField("ctr", "FLOAT"),
                                        bigquery.SchemaField(
                                            "engagement_rate_ranking", "STRING"
                                        ),
                                        bigquery.SchemaField("frequency", "FLOAT"),
                                        bigquery.SchemaField("impressions", "INTEGER"),
                                        bigquery.SchemaField("objective", "STRING"),
                                        bigquery.SchemaField(
                                            "optimization_goal", "STRING"
                                        ),
                                        bigquery.SchemaField(
                                            "place_page_name", "STRING"
                                        ),
                                        bigquery.SchemaField(
                                            "quality_ranking", "STRING"
                                        ),
                                        bigquery.SchemaField("reach", "INTEGER"),
                                        bigquery.SchemaField("social_spend", "FLOAT"),
                                        bigquery.SchemaField("spend", "FLOAT"),
                                        bigquery.SchemaField(
                                            "outbound_clicks", "INTEGER"
                                        ),
                                        bigquery.SchemaField(
                                            "outbound_clicks_ctr", "STRING"
                                        ),
                                        bigquery.SchemaField(
                                            "inline_link_click_ctr", "FLOAT"
                                        ),
                                        bigquery.SchemaField(
                                            "video_30_sec_watched_actions", "INTEGER"
                                        ),
                                        bigquery.SchemaField(
                                            "inline_link_clicks", "INTEGER"
                                        ),
                                    ],
                                    source_format=bigquery.SourceFormat.CSV,
                                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                    skip_leading_rows=1,
                                    allow_quoted_newlines=True,
                                    time_partitioning=bigquery.TimePartitioning(
                                        type_=bigquery.TimePartitioningType.DAY,
                                        field="date_start",
                                    ),
                                    clustering_fields=[
                                        "account_id",
                                        "ad_id",
                                        "adset_id",
                                        "campaign_id",
                                    ],
                                )

                                with open(csv_file_ordered, "rb") as source_file:
                                    job = client.load_table_from_file(
                                        source_file, table_id, job_config=job_config
                                    )

                                # print(job.result())

                                # Remove csv file
                                os.remove(csv_file_ordered)
                                # print(f"[INFO] Deleted {csv_file_ordered}.")

                                # Remove csv file
                                os.remove(csv_file_raw)
                                # print(f"[INFO] Deleted {csv_file_raw}.")

                                print(
                                    f"Job completed and sent to BQ: ad_account_id:{account_id}, report_run_id:{report_run_id}, date_start:{date_start}, date_stop:{date_stop}"
                                )

                            # Delete the record status
                            del status_list[i]
                            # Wait for 2 seconds
                            time.sleep(2)
                        else:
                            print(
                                f"ad_account_id:{account_id}, report_run_id:{report_run_id}, async_status:{async_status}, start_date:{date_start}, end_date:{date_stop}, status:{async_status}, run_count:{run_count}, run_interval:{run_interval}, index:{i}"
                            )
                            raise ValueError(f"Failed. No conditions met!")

            except ValueError as error:
                print("Error: ", error)
                print(f"Current ad_account_id: {ad_account_id}")


# Define task
get_facebook_insights = PythonOperator(
    task_id="get_facebook_insights",
    provide_context=False,
    depends_on_past=False,
    python_callable=get_facebook_insights,
    dag=dag,
)

# Define DAG :
START >> get_facebook_insights >> FINISH
