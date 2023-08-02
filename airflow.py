import os
import pandas as pd
import ast
import datetime

# from datetime import timezone
import croniter

# import pandas_gbq

from google.cloud.exceptions import NotFound
import airflow
import json
import requests

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook  # noqa:  F401
from notifications import Pagerduty as pagerduty


proj = ""
wop_stage = os.environ['WOP_STAGE']
wop_region = os.environ['WOP_REGION']
if wop_stage == "production":
    proj = "prod"
elif wop_stage == "staging":
    proj = "test"

openIncidents = {}

# LongrunningDAG max dag runtime allowance
dag_max_runhour = 8


# The get_lineage function gets all the monitored tables connected to dags (Dag names
# collected through pipline google sheets connected to gbq tables) through lineage
# and its suflow schedules for freshness calculation in get_last_modified
def get_lineage(**kwargs):

    from airflow_dag_utilities.bigquery import dataobs_bigquery_client as bq

    client = bq.Client(project="geotab-bigdata")
    table_lineage = f"""
        DECLARE PipelineList ARRAY<STRING> DEFAULT 
        (select ARRAY_AGG(PipelineName) from `geotab-dataobservability-{proj}.ProductAssets.ProductPipelines_MasterList` 
        where  DATE(_PARTITIONTIME) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY));

        DECLARE Embedded_Tables DEFAULT ARRAY<STRING> 
        ['geotab-embedded-prod.Summary_EU.DeviceCurrentStatusDaily',
            'geotab-embedded-prod.Vin_EU.DeviceVinStagingMonthlyDaily',
            'geotab-embedded-prod.Firmware_EU.DeviceFirmwareUpdate',
            'geotab-embedded-prod.Vin_EU.VinDecoder',
            'geotab-embedded-prod.AllDeviceData_EU.EngineStatus',
            'geotab-embedded-prod.AllDeviceData_EU.Debug',
            'geotab-embedded-prod.AllDeviceData_EU.EnhancedEngineFault',
            'geotab-embedded-prod.Reset_EU.DeviceReset',
            'geotab-embedded-prod.Trips_EU.DeviceTrips',
            'geotab-embedded-prod.Fuel_EU.DeviceFuel',
            'geotab-embedded-prod.EngineStatus_EU.StatusDataCountsDaily'];

        WITH DAG_Lineage AS (
            select * from `geotab-metadata-prod.Lineage.PipelinetoTable_Function`(PipelineList)
            WHERE Location = 'op-{wop_region}-{wop_stage}'
            AND REGEXP_CONTAINS(DestinationTable, r'[Ss]taging_BadDevicesFreeze') is false  
            and Runtag = "MostRecent"
        )
        ,cust_table as(
            SELECT pd.PipelineName, adv.TaskID, Location, DestinationTable,ProductName, 
            FROM `geotab-dataobservability-{proj}.ProductAssets.ProductPipelines_MasterList` pd
            LEFT JOIN DAG_Lineage adv ON pd.PipelineName = adv.PipelineName
            WHERE DATE(_PARTITIONTIME) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
            UNION ALL
            (
              SELECT PipelineName, TaskID, Location, DestinationTable, 'Embedded Production' ProductName
              FROM (SELECT * FROM `geotab-metadata-prod.Lineage.TabletoPipeline_Function`(Embedded_Tables) WHERE Runtag = "MostRecent")
              WHERE PipelineName IS NOT NULL
            )
        )
        , schedule_int as (
            select dag_id, schedule_interval, CurrentTimestamp
            from  `geotab-suflow-raw.op_eu_{wop_stage}_metadata.dag`
            where DATE(CurrentTimestamp) >= date_sub(CURRENT_DATE(), interval 60 day) and schedule_interval is not null
            group by dag_id,schedule_interval, CurrentTimestamp
            qualify row_number() over (PARTITION BY dag_id ORDER BY CurrentTimestamp DESC) = 1
        )
        select A.PipelineName as PipelineId, A.DestinationTable, A.TaskID, B.schedule_interval, A.ProductName
        from cust_table A
        Inner join schedule_int B
        ON A.PipelineName = B.dag_id
        group by 1,2,3,4,5

    """  # noqa E501

    Table_Lineage_Data = client.query(table_lineage).to_dataframe(
        create_bqstorage_client=False
    )
    Table_Lineage_Data = Table_Lineage_Data.dropna().reset_index()
    get_last_modified(Table_Lineage_Data, client)
    if wop_stage == "production" and wop_region == "eu":
        get_its_tables(client)


# The get_upstream_tables function gets all the upstream monitored tables connected to
# dags (Dag names collected through pipline google sheets connected to gbq tables) through
# lineage and its suflow schedules for freshness calculation in get_last_modified
def get_upstream_tables(**kwargs):

    from airflow_dag_utilities.bigquery import dataobs_bigquery_client as bq

    client = bq.Client(project="geotab-bigdata")

    table_lineage = f"""
        DECLARE PipelineList ARRAY<STRING> DEFAULT 
        (select ARRAY_AGG(PipelineName) from `geotab-dataobservability-prod.ProductAssets.UpstreamProductPipelines` 
        where  DATE(_PARTITIONTIME) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY));
        WITH DAG_Lineage AS (
            select * from `geotab-metadata-prod.Lineage.PipelinetoTable_Function`(PipelineList)
            WHERE Location = 'op-{wop_region}-{wop_stage}'
            AND REGEXP_CONTAINS(DestinationTable, r'[Ss]taging_BadDevicesFreeze') is false  
            and Runtag = "MostRecent"
        )
        ,cust_table as(
            SELECT pd.PipelineName, adv.TaskID, Location, DestinationTable,STRING_AGG(DISTINCT ProductName Order By ProductName) ProductName, 
            FROM `geotab-dataobservability-{proj}.ProductAssets.UpstreamProductPipelines` pd
            LEFT JOIN DAG_Lineage adv ON pd.PipelineName = adv.PipelineName
            WHERE DATE(_PARTITIONTIME) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) and IsProductDag = False
            group by 1,2,3,4
        )
        , schedule_int as (
            select dag_id, schedule_interval, CurrentTimestamp
            from  `geotab-suflow-raw.op_eu_production_metadata.dag`
            where DATE(CurrentTimestamp) >= date_sub(CURRENT_DATE(), interval 60 day) and schedule_interval is not null
            and dag_id in unnest(PipelineList)
            group by dag_id,schedule_interval, CurrentTimestamp
            qualify row_number() over (PARTITION BY dag_id ORDER BY CurrentTimestamp DESC) = 1
        )
        select A.PipelineName as PipelineId, A.DestinationTable, A.TaskID, B.schedule_interval, A.ProductName
        from cust_table A
        Inner join schedule_int B
        ON A.PipelineName = B.dag_id
        Where ProductName != "ITS"
        group by 1,2,3,4,5
    """  # noqa E501
    UpstreamTableData = client.query(table_lineage).to_dataframe(
        create_bqstorage_client=False
    )

    def get_customer_tables_from_sheet(FinalTableData, client):
        # tblList = pd.DataFrame()
        for i in FinalTableData.index:
            if FinalTableData['DestinationTable'][i] is None:
                tables = f"""
                    select ProductName, PipelineName, CustomerFacingTable
                    from `geotab-dataobservability-{proj}.ProductAssets.UpstreamProductPipelines`
                    where _PARTITIONTIME >= TIMESTAMP_SUB(Current_Timestamp(), interval 1 DAY) and IsProductDag = False and PipelineName = "{FinalTableData["PipelineId"][i]}"
                """  # noqa E501
                Tables = client.query(tables).to_dataframe(create_bqstorage_client=False)
                for k in Tables.index:
                    if Tables['CustomerFacingTable'][k] is not None:
                        tbl = Tables['CustomerFacingTable'][k].split(',')
                        for j in tbl:
                            j = j.strip()
                            FinalTableData = FinalTableData.append(
                                {
                                    "PipelineId": FinalTableData["PipelineId"][i],
                                    "DestinationTable": j,
                                    "TaskID": "",
                                    "schedule_interval": FinalTableData[
                                        "schedule_interval"
                                    ][i],
                                    "ProductName": FinalTableData["ProductName"][i],
                                },
                                ignore_index=True,
                            )

            # FinalTableData.drop(i, axis=0, inplace=True)
        return FinalTableData

    Table_Lineage_Data = get_customer_tables_from_sheet(UpstreamTableData, client)
    Table_Lineage_Data = get_sharded_tables(Table_Lineage_Data, client)
    Table_Lineage_Data = Table_Lineage_Data.dropna().reset_index()
    # test
    print("Table_Lineage_Data", Table_Lineage_Data)
    get_last_modified(Table_Lineage_Data, client, upstream=True)


def get_sharded_tables(FinalTableData, client):
    shards = []
    for i in FinalTableData.index:
        try:
            if FinalTableData["DestinationTable"][i] is not None:
                if "INFORMATION_SCHEMA" not in FinalTableData["DestinationTable"][i]:
                    client.get_table(FinalTableData["DestinationTable"][i])
        except NotFound:
            shards.append(
                [
                    FinalTableData["DestinationTable"][i],
                    FinalTableData["ProductName"][i],
                ]
            )
            FinalTableData.drop(labels=i, axis=0, inplace=True)

    shardData = pd.DataFrame(columns=['DestinationTable'])
    j = 0
    for i in range(len(shards)):
        print(shards[i])
        destTable = shards[i][0].split(".")
        project = destTable[0]
        dataset = destTable[1]
        table = destTable[2]
        shard = f"""
        SELECT SUBSTR(MAX(table_id), LENGTH('{table}') + 1) as latestShard
        FROM `{project}.{dataset}.__TABLES_SUMMARY__`
        WHERE table_id LIKE '{table}%' or table_id like "{table}$%"

        """
        try:
            shard_data = client.query(shard).to_dataframe(create_bqstorage_client=False)
        except NotFound:
            continue

        if shard_data["latestShard"][0] is not None and str.isnumeric(
            shard_data["latestShard"][0]
        ):
            shardData = shardData.append(
                {
                    "PipelineId": FinalTableData["PipelineId"][i],
                    "DestinationTable": str(
                        project
                        + "."
                        + dataset
                        + "."
                        + table
                        + "_2"
                        + shard_data["latestShard"][0]
                    ),
                    "schedule_interval": FinalTableData["schedule_interval"][i],
                    "ProductName": FinalTableData["ProductName"][i],
                },
                ignore_index=True,
            )
            j = j + 1

    FinalTableData = FinalTableData.append(shardData, ignore_index=True)
    return FinalTableData


def get_its_tables(client):
    itssql = f"""
        With ITSTable as(
            SELECT * Except(t, Comments, IsProductDag) REPLACE(t AS CustomerFacingTable)
            FROM `geotab-dataobservability-{proj}.ProductAssets.UpstreamProductPipelines` ,
            UNNEST(SPLIT(CustomerFacingTable)) t
            WHERE DATE(_PARTITIONTIME) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) and ProductName = "ITS"
        )
        , schedule_int as (
            select dag_id, schedule_interval, CurrentTimestamp
            from  `geotab-suflow-raw.op_eu_production_metadata.dag`
            where DATE(CurrentTimestamp) >= date_sub(CURRENT_DATE(), interval 60 day) and schedule_interval is not null
            group by dag_id,schedule_interval, CurrentTimestamp
            qualify row_number() over (PARTITION BY dag_id ORDER BY CurrentTimestamp DESC) = 1
        )
        select A.PipelineName as PipelineId, TRIM(A.CustomerFacingTable) As DestinationTable, B.schedule_interval, A.ProductName
        from ITSTable A
        Left join schedule_int B On A.PipelineName = B.dag_id
    """  # noqa E501
    ITSTableData = client.query(itssql).to_dataframe(create_bqstorage_client=False)
    get_last_modified(ITSTableData, client)


# The get_last_modified function gets the last modified date of each table
# and identifies unfresh table through the last modified date
def get_last_modified(Table_Lineage_Data, client, upstream=False):
    lst = []
    isfrsh = []
    sched = []
    tmediff = []
    # create dict daglst to keep track of checked pipelines
    # along with its dag record length
    daglst = {"PipelineName": [], "dag_record_len": []}
    now = datetime.datetime.now(datetime.timezone.utc)
    TableData = pd.DataFrame()
    for i in Table_Lineage_Data.index:
        # print(Upstream_Lineage_Data['DestinationTable'][i])
        if ":" not in Table_Lineage_Data['DestinationTable'][i]:
            dest = Table_Lineage_Data['DestinationTable'][i].split('.')
            project = dest[0].strip()
            dataset = dest[1].strip()
            table = dest[2].split("$")[0].strip()

        else:
            dest = Table_Lineage_Data['DestinationTable'][i].split(':')
            project = dest[0].strip()
            dtstbl = dest[1].split(".")
            dataset = dtstbl[0].strip()
            table = dtstbl[1].split("$")[0].strip()

        table_id = f"{project}.{dataset}.{table}"

        try:
            table = client.get_table(str(table_id))
        except NotFound:
            sched.append(None)
            lst.append(None)
            tmediff.append(None)
            isfrsh.append(None)
            continue

        suflow_sched = Table_Lineage_Data["schedule_interval"][i].strip('"')

        schedule = croniter.croniter(suflow_sched, now)
        sched.append(schedule.get_prev(datetime.datetime))
        lst.append(table.modified)
        # assign Pipelineid to variable PipelineName
        PipelineName = Table_Lineage_Data['PipelineId'][i]
        # print(i, len(lst),len(sched))
        timediff = lst[i].replace(microsecond=0) - sched[i].replace(microsecond=0)
        tmediff.append(str(timediff))

        if timediff.days >= 0 and timediff.seconds >= 0:
            isfrsh.append("True")
            # print(len(lst), len(sched), len(tmediff))
            continue
        # add get_dag_states to rule out and log paused dags
        elif get_dag_states(Table_Lineage_Data['PipelineId'][i]) is True:
            log(
                Table_Lineage_Data['DestinationTable'][i],
                Table_Lineage_Data['PipelineId'][i],
                upstream,
                "DAG paused: " + Table_Lineage_Data['PipelineId'][i],
                "DAGPaused",
            )
            isfrsh.append("True")
            continue

        # check if dag is 1.running 2. longrunning 3. update and return dag list
        dag_running_indic, daglst = update_dag_runrecord(PipelineName, daglst)
        if dag_running_indic == 11:
            log(
                Table_Lineage_Data['DestinationTable'][i],
                Table_Lineage_Data['PipelineId'][i],
                upstream,
                "Long running DAG: " + Table_Lineage_Data['PipelineId'][i],
                "LongRunningDAG",
            )
            # is freshness still holds true?
            isfrsh.append("True")
        elif dag_running_indic == 10:
            # print(dag_run_record_length, daglst)
            # identify long running DAG function
            log(
                Table_Lineage_Data['DestinationTable'][i],
                Table_Lineage_Data['PipelineId'][i],
                upstream,
                "DAG currently running: " + Table_Lineage_Data['PipelineId'][i],
                "DAGRunning",
            )
            isfrsh.append("True")
        elif Table_Lineage_Data['ProductName'][i] == "ITS":
            isfrsh.append("False")
        elif (
            check_task_status(
                Table_Lineage_Data['PipelineId'][i],
                Table_Lineage_Data['TaskID'][i],
            )
            == "skipped"
        ):
            log(
                Table_Lineage_Data['DestinationTable'][i],
                Table_Lineage_Data['PipelineId'][i],
                upstream,
                "Task Skipped: " + Table_Lineage_Data['TaskID'][i],
                "TaskSkipped",
            )
            isfrsh.append("True")
        else:
            isfrsh.append("False")

    TableData["TableNames"] = Table_Lineage_Data['DestinationTable'].to_list()
    TableData["Pipeline"] = Table_Lineage_Data['PipelineId'].to_list()
    TableData["Product"] = Table_Lineage_Data['ProductName'].to_list()
    TableData["LastModified"] = lst
    TableData["ScheduledRun"] = sched
    TableData["TimeDifference"] = tmediff
    TableData["IsFresh"] = isfrsh

    TableData.to_gbq(
        "Freshness.Freshness",
        project_id=f"geotab-dataobservability-{proj}",
        chunksize=10000,
        reauth=False,
        if_exists='append',
        progress_bar=True,
        credentials=None,
    )
    get_outdated_list_send_alerts(TableData, upstream)


# The get_outdated_list_send_alerts function selects the unfresh data
# and triggers the send_alert function
def get_outdated_list_send_alerts(Freshness_Data, upstream=False):

    outdated = (
        Freshness_Data[Freshness_Data['IsFresh'] == 'False']
        .groupby('Pipeline')[['TableNames', 'Product', 'LastModified']]
        .apply(lambda x: x.values.tolist())
        .to_dict()
    )
    print('outdated', outdated)
    send_alerts(outdated, upstream)


def vault_access():
    from data_obs_freshness.HelperFiles.vault_helper import VaultCredentialAdapter

    x = json.dumps(ast.literal_eval(BaseHook.get_connection("dataobs_vault").password))
    vault_token = ast.literal_eval(x)

    vault = VaultCredentialAdapter(service_account_file=vault_token)
    if vault.is_client_authenticated():
        return vault
    else:
        return None


def check_task_status(dagId, taskId, state="success"):
    from airflow.models import DagRun  # , DagBag

    dagRun = DagRun(dag_id=dagId)
    dag = dagRun.find(dag_id=dagId, state=state)
    if len(dag) > 0:
        task = dag[len(dag) - 1].get_task_instance(task_id=taskId)
        if task is not None:
            return task.state

    return None


# def check_runningdag(dag_run_record, PipelineName):
#     if dag_run_record == 'DAG not running':
#         dag_running_indic = 0
#     else:
#         dag_running_indic = 1
#         print("DAG currently running: " + PipelineName)
#         try:
#             dag_longrunning_indic = check_longrunningdag(dag_run_record)
#         except ValueError:
#             print('long running DAG check failed')
#     return dag_running_indic, dag_longrunning_indic


# get_dag_states function determines if a DAG is paused or not
def get_dag_states(dagId):
    from airflow.models import dag

    dag_state = dag.DAG(dag_id=dagId).get_is_paused()
    # get_states = DagModel.get_paused_dag_ids(dag_id)
    # test
    print(dag, 'dag_is_paused', dag_state)
    return dag_state


def get_dag_runs(dag_id):
    from airflow.models import DagRun  # , DagBag

    get_latest_dag_runs = DagRun.find(dag_id=dag_id)
    latest_dag_runs = sorted(
        get_latest_dag_runs, key=lambda x: x.execution_date, reverse=True
    )[:2]
    if latest_dag_runs[0].state == 'running':
        dag_runs = latest_dag_runs
    else:
        dag_runs = 'DAG not running'
    return dag_runs


def update_dag_runrecord(PipelineName, daglst):
    dag_longrunning_indic = dag_running_indic = 0
    if PipelineName in daglst['PipelineName']:
        # if exists, use the index of existing pipeline
        # to get dag_record_len from daglst
        dag_running_indic = daglst['dag_record_len'][
            daglst['PipelineName'].index(PipelineName)
        ]
    else:
        # if doesn't exist, get the dag info from get_dag_runs() and append
        # the pipelineid and length of dag_run_record
        dag_run_record = get_dag_runs(PipelineName)
        if dag_run_record == 'DAG not running':
            dag_running_indic = 00
        else:
            print("DAG currently running: " + PipelineName)
            try:
                dag_longrunning_indic = check_longrunningdag(dag_run_record)
            except ValueError:
                print('long running DAG check failed')
            if dag_longrunning_indic == 1:
                dag_running_indic = 11
            else:
                dag_running_indic = 10
    daglst["PipelineName"].append(PipelineName)
    daglst["dag_record_len"].append(dag_running_indic)
    return dag_running_indic, daglst


def check_longrunningdag(dag_runs):
    # check if dag is long-running
    # dag_runs = get_dag_runs(dagId)
    dag_longrunning_indic = 0
    # Step2: check previous dag state
    if dag_runs[1].state == 'running':
        print('previous state = running, long running dag')
        dag_longrunning_indic = 1
    else:
        if dag_runs[1].state == 'success':
            print('previous dagrun was successful, proceed to step4')
            # Jump to Step4: check current dagrun time
            dag_longrunning_indic = check_current_dagruntime(dag_runs)
        else:
            # test
            print('previous dagrun state is:', dag_runs[1].state)
            if (
                dag_runs[1].execution_date
                and dag_runs[1].end_date
                and dag_runs[1].start_date
            ):
                exe_period = dag_runs[0].execution_date - dag_runs[1].execution_date
                exe_duration = dag_runs[1].end_date - dag_runs[1].start_date
                period_min = int(round(exe_period.total_seconds() / 60))
                duration_min = int(round(exe_duration.total_seconds() / 60))
                # test
                print(
                    'previous execution period is:',
                    period_min,
                    'minutes; previous dagrun runtime is:',
                    duration_min,
                    'minutes',
                )
                # Step3: check if previous dag timed out
                if period_min - duration_min <= 1:
                    print('dag timed out, long running dag')
                    dag_longrunning_indic = 1
                else:
                    # Step4: check current dagrun time
                    dag_longrunning_indic = check_current_dagruntime(dag_runs)
            else:
                # Step4: check current dagrun time
                dag_longrunning_indic = check_current_dagruntime(dag_runs)
    return dag_longrunning_indic


def check_current_dagruntime(dag_runs):
    dag_longrunning_indic = 0
    if dag_runs[0].end_date:
        dagrun_endtime = dag_runs[0].end_date
    else:
        dagrun_endtime = datetime.datetime.now(datetime.timezone.utc)
        current_duration = dagrun_endtime - dag_runs[0].start_date
        duration_hr = int(round(current_duration.total_seconds() / 3600))
        # test
        print('current dagrun runtime is:', duration_hr, 'hour')
        if duration_hr >= dag_max_runhour:
            print('dag ran pass time limit, long running dag')
            dag_longrunning_indic = 1
        else:
            print('All checks passed, not a long running dag')
    return dag_longrunning_indic


def get_key(vault, key):
    if vault is not None:
        keySecret = vault.retrieve_service_account_secrets_from_vault(
            'DNA-DataObservability-app-PagerDuty-secret/' + key
        )
    else:
        raise ValueError("Vault access not authenticated.")
    return keySecret


def get_service_ids(products, rest_api_token, limit="100"):
    url = "https://api.pagerduty.com/services"
    querystring = {"limit": "100"}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.pagerduty+json;version=2",
        "Authorization": f"Token token={rest_api_token}",
    }
    filtered_dict = {}
    try:
        response = requests.request("GET", url, headers=headers, params=querystring)
        response_dict = response.json()
        for i in response_dict['services']:
            if i["name"] in products:
                filtered_dict[i["name"]] = i["id"]
    except ValueError:
        filtered_dict = {}
    return filtered_dict


# The send_alerts function creates open incidents for unfresh tables
# and sends all incident alerts through PagerDuty
def send_alerts(outdated, upstream=False, **kwargs):

    from airflow_dag_utilities.bigquery import dataobs_bigquery_client as bq

    vault = vault_access()
    client = bq.Client(project="geotab-bigdata")
    Get_Service = (
        f"""select * from `geotab-dataobservability-{proj}.Freshness.PagerdutyServices`"""
    )
    All_Service = client.query(Get_Service).to_dataframe()
    # default to airflow
    SERVICE_IDS = [
        All_Service.ServiceIDs[All_Service.ServiceNames.tolist().index('Airflow')]
    ]
    rest_api_token = get_key(vault, "restapitoken")

    if upstream is False:
        for k in outdated:
            if outdated[k]:
                if outdated[k][0][1] not in All_Service.ServiceNames.tolist():
                    NewService = get_service_ids(
                        [outdated[k][0][1]], rest_api_token["restapitoken"]
                    )
                    if NewService != {}:
                        NewService_df = pd.DataFrame(
                            NewService.items(), columns=['ServiceNames', 'ServiceIDs']
                        )
                        All_Service = All_Service.append(NewService_df, ignore_index=True)
                        NewService_df.to_gbq(
                            "Freshness.PagerdutyServices",
                            project_id=f"geotab-dataobservability-{proj}",
                            chunksize=10000,
                            reauth=False,
                            if_exists='append',
                            progress_bar=True,
                            credentials=None,
                        )
                        SERVICE_IDS.append(NewService[outdated[k][0][1]])
                else:
                    SERVICE_IDS.append(
                        All_Service.ServiceIDs[
                            All_Service.ServiceNames.tolist().index(outdated[k][0][1])
                        ]
                    )
    else:
        SERVICE_IDS.extend(All_Service.ServiceIDs.tolist())

    STATUSES = ["triggered", "acknowledged"]
    # get rid of duplicate
    SERVICE_IDS = list(set(SERVICE_IDS))
    # test
    print("SERVICE_IDS", SERVICE_IDS)
    link = f"https://op-{wop_region}-prod.geotab.com/tree?dag_id="

    # openIncidents = {}
    # Changing openincidents to a conditional function call
    # to differentiate upstream alerts

    # print(openIncidents)
    for pipeline in outdated:
        for tableInfo in outdated[pipeline]:
            exists = check_alerts(
                rest_api_token, SERVICE_IDS, STATUSES, pipeline, tableInfo, upstream
            )
            print(exists)
            if not exists:
                call_pd_alert(vault, tableInfo, pipeline, link, upstream)
        if outdated[pipeline]:
            if (
                not upstream
                and outdated[pipeline][0][1] in All_Service.ServiceNames.tolist()
            ):
                Service_ID = All_Service.ServiceIDs[
                    All_Service.ServiceNames.tolist().index(outdated[pipeline][0][1])
                ]
            else:
                Service_ID = All_Service.ServiceIDs[
                    All_Service.ServiceNames.tolist().index('Airflow')
                ]
            pagerduty.merge_all_incidents(
                rest_api_token["restapitoken"],
                pipeline,
                Service_ID,
                "nanyujiang@geotab.com",
            )


def check_alerts(rest_api_token, SERVICE_IDS, STATUSES, pipeline, tableInfo, upstream):
    openIncidents = pagerduty.get_all_incidents(
        rest_api_token["restapitoken"],
        SERVICE_IDS,
        STATUSES,
        lambda x: (x['title'] == pipeline, x['title']),
    )
    searchTablestring = f"Table {tableInfo[0]} has not been fresh"
    if openIncidents:
        print(openIncidents, searchTablestring)
        for i in openIncidents[pipeline]["alerts"]:
            if searchTablestring in i['summary']:
                log(
                    tableInfo[0],
                    pipeline,
                    upstream,
                    "Incident already created for: " + tableInfo[0],
                    "Incident Already Created",
                )
                return True
    else:
        openIncidents = pagerduty.get_all_incidents(
            rest_api_token["restapitoken"],
            SERVICE_IDS,
            STATUSES,
            lambda x: (searchTablestring in x['title'], x['title']),
        )
        if openIncidents:
            return True

    return False


def call_pd_alert(vault, tableInfo, pipeline, link, upstream):
    message = (
        "Table "
        + tableInfo[0]
        + " has not been fresh since "
        + str(tableInfo[2].replace(microsecond=0))
        + "."
    )

    # header = {"Content-Type": "application/json"}
    print(tableInfo[1])

    if upstream is False:
        intKey = get_key(vault, tableInfo[1])
        if intKey is not None:
            routeKey = intKey[tableInfo[1]]
        else:
            print(tableInfo[1])
            intKey = get_key(vault, "TestService")
            routeKey = intKey['TestService']
    else:
        intKey = get_key(vault, "TestService")
        routeKey = intKey['TestService']

    if wop_stage == "staging":
        intKey = get_key(vault, "DataObsTest")
        routeKey = intKey['DataObsTest']

    response = pagerduty.send_alerts(
        routeKey,
        proj,
        message,
        custom_details={
            "Table": tableInfo[0],
            # "Task": outdated[i][1],
            "Last Modified": str(tableInfo[2].replace(microsecond=0).isoformat()),
            "Pipeline/DAG": pipeline,
            "DAG Link": link + pipeline,
        },
    )
    if 'Incident created' in response:
        log(
            tableInfo[0],
            pipeline,
            upstream,
            "Incident created for: " + tableInfo[0] + " in Service: " + tableInfo[1],
            "Incident Created",
        )


def log(table, pipeline, upstream, message, logtype):

    logData = pd.DataFrame()
    logData = logData.append(
        {
            "Table": table,
            "Pipeline": pipeline,
            "Logtype": logtype,
            "Message": message,
            "Upstream": str(upstream),
            "Timestamp": datetime.datetime.utcnow(),
        },
        ignore_index=True,
    )

    logData.to_gbq(
        "Freshness.Logs",
        project_id=f"geotab-dataobservability-{proj}",
        chunksize=10000,
        reauth=False,
        if_exists='append',
        progress_bar=True,
        credentials=None,
    )


def longrundaglog(pipeline, message, logtype):

    logData = pd.DataFrame()
    logData = logData.append(
        {
            "Pipeline": pipeline,
            "Logtype": logtype,
            "Message": message,
            "Timestamp": datetime.datetime.utcnow(),
        },
        ignore_index=True,
    )

    logData.to_gbq(
        "Freshness.Longrundag_Logs",
        project_id=f"geotab-dataobservability-{proj}",
        chunksize=10000,
        reauth=False,
        if_exists='append',
        progress_bar=True,
        credentials=None,
    )


def main():
    sched_int = None
    email = ["nanyujiang@geotab.com"]
    if wop_stage == "production":
        sched_int = "19 * * * *"
        email = [
            "samanthawarring@geotab.com",
            "jonathanstolle@geotab.com",
        ]

    dag_args = {
        # DAG
        # department-project_name-dag_description (inherit from definition)
        "dag_id": os.path.splitext(os.path.basename(__file__))[0],
        "schedule_interval": sched_int,
        "max_active_runs": 1,
        "dagrun_timeout": datetime.timedelta(minutes=59),
        "template_searchpath": ["/home/airflow/gcs/dags/data_obs_freshness/"],
        "catchup": False,  # must include because airflow default catchup is True
        # Operators
        "default_args": {
            "start_date": datetime.datetime(2022, 3, 8, 15),  # (year, m,  d, hour)
            # "start_date": None,  # time: UTC, uses dag_utils.suggest_start_date
            "owner": "Data Observability",
            "email": email,
            "depends_on_past": False,  # new instance will not run if past job failed
            "retries": 1,
            "retry_delay": datetime.timedelta(seconds=120),
            "email_on_failure": True,
            "email_on_retry": False,
            # BigQueryOperator
            "write_disposition": "WRITE_TRUNCATE",
            "use_legacy_sql": False,
            "time_partitioning": {"type": "DAY"},
            "priority": "BATCH",
        },
    }
    # dag_args["default_args"]["start_date"] = dag_utils.suggest_start_date(
    #     dag_args["schedule_interval"], intervals=-2
    # )  # remove automatic start_date assignment when using your own

    create_dag(dag_args)


def create_dag(dag_args):
    with airflow.DAG(**dag_args) as dag:
        """
        No logic outside of tasks(operators) or they are constantly run by scheduler
        """

        task_start_dag = DummyOperator(task_id='start_dag', dag=dag)

        task_get_customer_facing_table_data = PythonOperator(
            task_id='get_customer_facing_table_data',
            python_callable=get_lineage,
            dag=dag,
        )
        task_get_upstream_table_data = PythonOperator(
            task_id='get_upstream_table_data',
            python_callable=get_upstream_tables,
            dag=dag,
        )

        task_end_dag = DummyOperator(task_id='end_dag', dag=dag)

        (
            task_start_dag
            >> task_get_customer_facing_table_data
            >> task_get_upstream_table_data
            >> task_end_dag
        )

    globals()[dag.dag_id] = dag  # keep this


main()  # keep this
