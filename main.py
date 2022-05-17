import json
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest
from datetime import datetime, date, timedelta
import pandas
import requests
import logging
import base64
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights


logger = logging.getLogger()

schema_facebook_stat_ad = [
    bigquery.SchemaField('actions', 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("account_currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("account_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ad_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ad_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("adset_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("adset_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("buying_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("conversions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_15_sec_video_view", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_conversion", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_estimated_ad_recallers",
                         "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_inline_link_click",
                         "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_outbound_click", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_thruplay", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_unique_action_type", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_unique_click", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_unique_inline_link_click",
                         "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_unique_outbound_click", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cpc", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cpm", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cpp", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ctr", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("date_start", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("date_stop", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("estimated_ad_recallers", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("frequency", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("mobile_app_purchase_roas", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("objective", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("optimization_goal", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("outbound_clicks_ctr", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("outbound_clicks", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("purchase_roas", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("reach", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("spend", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("unique_clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("unique_ctr", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("unique_video_view_15_sec", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_15_sec_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_30_sec_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_avg_time_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p100_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p25_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p50_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p75_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p95_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_play_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("website_ctr", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("website_purchase_roas", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING')))
]

schema_facebook_stat_campaign = [
    bigquery.SchemaField('actions', 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("account_currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("account_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("buying_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("conversions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_15_sec_video_view", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_conversion", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_estimated_ad_recallers",
                         "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_inline_link_click",
                         "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_outbound_click", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_thruplay", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_unique_action_type", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cost_per_unique_click", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_unique_inline_link_click",
                         "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost_per_unique_outbound_click", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("cpc", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cpm", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cpp", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ctr", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("date_start", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("date_stop", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("estimated_ad_recallers", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("frequency", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("mobile_app_purchase_roas", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("objective", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("optimization_goal", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("outbound_clicks_ctr", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("outbound_clicks", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("purchase_roas", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("reach", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("spend", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("unique_clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("unique_ctr", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("unique_video_view_15_sec", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_15_sec_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_30_sec_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_avg_time_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p100_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p25_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p50_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p75_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_p95_watched_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("video_play_actions", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("website_ctr", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField("website_purchase_roas", 'RECORD', mode='REPEATED', fields=(
        bigquery.SchemaField('action_type', 'STRING'), bigquery.SchemaField('value', 'STRING')))
]

clustering_fields_facebook = ['campaign_id', 'campaign_name']

array_fields = [
    'actions',
    'conversions',
    'cost_per_15_sec_video_view',
    'cost_per_conversion',
    'cost_per_outbound_click',
    'cost_per_thruplay',
    'cost_per_unique_action_type',
    'cost_per_unique_outbound_click',
    'mobile_app_purchase_roas',
    'outbound_clicks_ctr',
    'outbound_clicks',
    'purchase_roas',
    'unique_video_view_15_sec',
    'video_15_sec_watched_actions',
    'video_30_sec_watched_actions',
    'video_avg_time_watched_actions',
    'video_p100_watched_actions',
    'video_p25_watched_actions',
    'video_p50_watched_actions',
    'video_p75_watched_actions',
    'video_p95_watched_actions',
    'video_play_actions',
    'website_ctr',
    'website_purchase_roas'
]

fields_ad = [
    AdsInsights.Field.account_currency,
    AdsInsights.Field.account_id,
    AdsInsights.Field.account_name,
    AdsInsights.Field.actions,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.ad_name,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.adset_name,
    AdsInsights.Field.buying_type,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.clicks,
    AdsInsights.Field.conversions,
    AdsInsights.Field.cost_per_15_sec_video_view,
    AdsInsights.Field.cost_per_conversion,
    AdsInsights.Field.cost_per_estimated_ad_recallers,
    AdsInsights.Field.cost_per_inline_link_click,
    AdsInsights.Field.cost_per_outbound_click,
    AdsInsights.Field.cost_per_thruplay,
    AdsInsights.Field.cost_per_unique_action_type,
    AdsInsights.Field.cost_per_unique_click,
    AdsInsights.Field.cost_per_unique_inline_link_click,
    AdsInsights.Field.cost_per_unique_outbound_click,
    AdsInsights.Field.cpc,
    AdsInsights.Field.cpm,
    AdsInsights.Field.cpp,
    AdsInsights.Field.ctr,
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.estimated_ad_recallers,
    AdsInsights.Field.frequency,
    AdsInsights.Field.impressions,
    AdsInsights.Field.mobile_app_purchase_roas,
    AdsInsights.Field.objective,
    AdsInsights.Field.optimization_goal,
    AdsInsights.Field.outbound_clicks_ctr,
    AdsInsights.Field.outbound_clicks,
    AdsInsights.Field.purchase_roas,
    AdsInsights.Field.reach,
    AdsInsights.Field.spend,
    AdsInsights.Field.unique_clicks,
    AdsInsights.Field.unique_ctr,
    AdsInsights.Field.unique_video_view_15_sec,
    AdsInsights.Field.video_15_sec_watched_actions,
    AdsInsights.Field.video_30_sec_watched_actions,
    AdsInsights.Field.video_avg_time_watched_actions,
    AdsInsights.Field.video_p100_watched_actions,
    AdsInsights.Field.video_p25_watched_actions,
    AdsInsights.Field.video_p50_watched_actions,
    AdsInsights.Field.video_p75_watched_actions,
    AdsInsights.Field.video_p95_watched_actions,
    AdsInsights.Field.video_play_actions,
    AdsInsights.Field.website_ctr,
    AdsInsights.Field.website_purchase_roas
]

fields_campaign = [
    AdsInsights.Field.account_currency,
    AdsInsights.Field.account_id,
    AdsInsights.Field.account_name,
    AdsInsights.Field.actions,
    AdsInsights.Field.buying_type,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.clicks,
    AdsInsights.Field.conversions,
    AdsInsights.Field.cost_per_15_sec_video_view,
    AdsInsights.Field.cost_per_conversion,
    AdsInsights.Field.cost_per_estimated_ad_recallers,
    AdsInsights.Field.cost_per_inline_link_click,
    AdsInsights.Field.cost_per_outbound_click,
    AdsInsights.Field.cost_per_thruplay,
    AdsInsights.Field.cost_per_unique_action_type,
    AdsInsights.Field.cost_per_unique_click,
    AdsInsights.Field.cost_per_unique_inline_link_click,
    AdsInsights.Field.cost_per_unique_outbound_click,
    AdsInsights.Field.cpc,
    AdsInsights.Field.cpm,
    AdsInsights.Field.cpp,
    AdsInsights.Field.ctr,
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.estimated_ad_recallers,
    AdsInsights.Field.frequency,
    AdsInsights.Field.impressions,
    AdsInsights.Field.mobile_app_purchase_roas,
    AdsInsights.Field.objective,
    AdsInsights.Field.optimization_goal,
    AdsInsights.Field.outbound_clicks_ctr,
    AdsInsights.Field.outbound_clicks,
    AdsInsights.Field.purchase_roas,
    AdsInsights.Field.reach,
    AdsInsights.Field.spend,
    AdsInsights.Field.unique_clicks,
    AdsInsights.Field.unique_ctr,
    AdsInsights.Field.unique_video_view_15_sec,
    AdsInsights.Field.video_15_sec_watched_actions,
    AdsInsights.Field.video_30_sec_watched_actions,
    AdsInsights.Field.video_avg_time_watched_actions,
    AdsInsights.Field.video_p100_watched_actions,
    AdsInsights.Field.video_p25_watched_actions,
    AdsInsights.Field.video_p50_watched_actions,
    AdsInsights.Field.video_p75_watched_actions,
    AdsInsights.Field.video_p95_watched_actions,
    AdsInsights.Field.video_play_actions,
    AdsInsights.Field.website_ctr,
    AdsInsights.Field.website_purchase_roas
]

def exist_dataset_table(client, table_id, dataset_id, project_id, schema, clustering_fields=None):

    try:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        client.get_dataset(dataset_ref)  # Make an API request.

    except NotFound:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        dataset = client.create_dataset(dataset)  # Make an API request.
        logger.info("Created dataset {}.{}".format(
            client.project, dataset.dataset_id))

    try:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        client.get_table(table_ref)  # Make an API request.

    except NotFound:

        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)

        table = bigquery.Table(table_ref, schema=schema)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date_start"
        )

        if clustering_fields is not None:
            table.clustering_fields = clustering_fields

        table = client.create_table(table)  # Make an API request.
        logger.info("Created table {}.{}.{}".format(
            table.project, table.dataset_id, table.table_id))

    return 'ok'


def insert_rows_bq(client, table_id, dataset_id, project_id, data):

    # print(data)

    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    table = client.get_table(table_ref)

    try:
        resp = client.insert_rows_json(
            json_rows=data,
            table=table_ref,
        )
    except BadRequest:
        print('No Rows present, skipping interval...')
    else:
        print(resp)
        logger.info("Success uploaded to table {}".format(table.table_id))


def wait_for_async_job(job):
    job.api_get()
    i = 0
    while job[AdReportRun.Field.async_status] != "Job Completed":
        time.sleep(5)
        i = i + 1
        print(f'{i * 5}s: Fetching Facebook Data...')
        job.api_get()
    return job.get_result(params={"limit": 1000})


def get_facebook_data(event, context):

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    bigquery_client = bigquery.Client()

    has_dates = 'date_since' in event['attributes'] and 'date_until' in event['attributes']

    table_id = event['attributes']['table_id']
    dataset_id = event['attributes']['dataset_id']
    project_id = event['attributes']['project_id']

    app_id = event['attributes']['app_id']
    app_secret = event['attributes']['app_secret']
    access_token = event['attributes']['access_token']
    account_id = event['attributes']['account_id']

    if pubsub_message == 'get_facebook':
        yesterday = date.today() - timedelta(1)

        try:
            FacebookAdsApi.init(app_id, app_secret,
                                access_token, api_version='v13.0')

            account = AdAccount('act_'+str(account_id))
            job = account.get_insights_async(fields=fields_ad, params={
                'level': 'ad',
                'time_range': {
                    'since':  event['attributes']['date_since'] if has_dates else yesterday.strftime("%Y-%m-%d"),
                    'until': event['attributes']['date_until'] if has_dates else yesterday.strftime("%Y-%m-%d")
                }, 'time_increment': 1
            })

            result_cursor = wait_for_async_job(job)
            insights = [item for item in result_cursor]

            df = pandas.DataFrame(insights)

            for item in array_fields:
                if item in df.columns:
                    df[item] = df[item].fillna('').apply(list)
                else:
                    df[item] = df.apply(lambda _: [], axis=1)

        except Exception as e:
            logger.info(e)
            print(e)
            raise

        if exist_dataset_table(bigquery_client, table_id, dataset_id, project_id, schema_facebook_stat_ad, clustering_fields_facebook) == 'ok':

            print('Inserting data to bigquery...')
            insert_rows_bq(bigquery_client, table_id,
                           dataset_id, project_id, json.loads(df.to_json(orient='records')))
            print('Done inserting...')

            return 'ok'
    
    elif pubsub_message == 'get_facebook_month':

        last_prev_month = date.today().replace(day=1) - timedelta(days=1)
        first_prev_month = date.today().replace(
            day=1) - timedelta(days=last_prev_month.day)

        if has_dates:
            print(
                f"Start: {event['attributes']['date_since']}\nEnd: {event['attributes']['date_until']}")
        else:
            print(f'Start: {first_prev_month}\nEnd: {last_prev_month}')

        try:
            FacebookAdsApi.init(app_id, app_secret,
                                access_token, api_version='v13.0')

            account = AdAccount('act_'+str(account_id))
            job = account.get_insights_async(fields=fields_campaign, params={
                'level': 'campaign',
                'time_range': {
                    'since':  event['attributes']['date_since'] if has_dates else first_prev_month.strftime("%Y-%m-%d"),
                    'until': event['attributes']['date_until'] if has_dates else last_prev_month.strftime("%Y-%m-%d")
                }, 'time_increment': 'monthly'
            })

            result_cursor = wait_for_async_job(job)
            insights = [item for item in result_cursor]

            df = pandas.DataFrame(insights)

            for item in array_fields:
                if item in df.columns:
                    df[item] = df[item].fillna('').apply(list)
                else:
                    df[item] = df.apply(lambda _: [], axis=1)

        except Exception as e:
            logger.info(e)
            print(e)
            raise

        if exist_dataset_table(bigquery_client, table_id, dataset_id, project_id, schema_facebook_stat_campaign, clustering_fields_facebook) == 'ok':

            print('Inserting data to bigquery...')
            insert_rows_bq(bigquery_client, table_id,
                           dataset_id, project_id, json.loads(df.to_json(orient='records')))
            print('Done inserting...')

            return 'ok'

    elif pubsub_message == 'get_facebook_week':

        last_week_sunday = date.today() - timedelta(days=date.today().weekday() + 1)
        last_week_monday = last_week_sunday - timedelta(days=6)

        print(f'Start: {last_week_monday}\nEnd: {last_week_sunday}')

        try:
            FacebookAdsApi.init(app_id, app_secret,
                                access_token, api_version='v13.0')

            account = AdAccount('act_'+str(account_id))
            job = account.get_insights_async(fields=fields_campaign, params={
                'level': 'campaign',
                'time_range': {
                    'since':  last_week_monday.strftime("%Y-%m-%d"),
                    'until': last_week_sunday.strftime("%Y-%m-%d")
                }, 'time_increment': 'all_days'
            })

            result_cursor = wait_for_async_job(job)
            insights = [item for item in result_cursor]

            df = pandas.DataFrame(insights)

            for item in array_fields:
                if item in df.columns:
                    df[item] = df[item].fillna('').apply(list)
                else:
                    df[item] = df.apply(lambda _: [], axis=1)

        except Exception as e:
            logger.info(e)
            print(e)
            raise

        if exist_dataset_table(bigquery_client, table_id, dataset_id, project_id, schema_facebook_stat_campaign, clustering_fields_facebook) == 'ok':

            print('Inserting data to bigquery...')
            insert_rows_bq(bigquery_client, table_id,
                           dataset_id, project_id, json.loads(df.to_json(orient='records')))
            print('Done inserting...')

            return 'ok'

    elif pubsub_message == 'get_facebook_ytd':

        first_day_of_year = date.today().replace(month=1, day=1)
        today = date.today()

        print(f'Start: {first_day_of_year}\nEnd: {today}')

        try:
            FacebookAdsApi.init(app_id, app_secret,
                                access_token, api_version='v13.0')

            account = AdAccount('act_'+str(account_id))
            job = account.get_insights_async(fields=fields_campaign, params={
                'level': 'campaign',
                'time_range': {
                    'since':  first_day_of_year.strftime("%Y-%m-%d"),
                    'until': today.strftime("%Y-%m-%d")
                }, 'time_increment': 'all_days'
            })

            result_cursor = wait_for_async_job(job)
            insights = [item for item in result_cursor]

            df = pandas.DataFrame(insights)

            for item in array_fields:
                if item in df.columns:
                    df[item] = df[item].fillna('').apply(list)
                else:
                    df[item] = df.apply(lambda _: [], axis=1)

        except Exception as e:
            logger.info(e)
            print(e)
            raise

        if exist_dataset_table(bigquery_client, table_id, dataset_id, project_id, schema_facebook_stat_campaign, clustering_fields_facebook) == 'ok':

            print('Inserting data to bigquery...')
            insert_rows_bq(bigquery_client, table_id,
                           dataset_id, project_id, json.loads(df.to_json(orient='records')))
            print('Done inserting...')

            return 'ok'

def getIntervalFacebookData(start: str, limit: int, interval: int):
    i = 0
    yesterday = date.today() - timedelta(days=1)

    start = datetime.strptime(start, '%Y-%m-%d')

    while i < limit:
        i = i + 1
        end = start + timedelta(days=interval)

        if end > yesterday:
            end = yesterday
            i = limit

        print(f'{i}. Intervall: {datetime.strftime(start, "%Y-%m-%d")} - {datetime.strftime(end, "%Y-%m-%d")}\n')

        evnt = {
            'attributes': {
                'table_id': 'raw_data',
                'dataset_id': 'facebook_marketing_api',
                'project_id': '',
                'app_id': '',
                'app_secret': '',
                'access_token': '',
                'account_id': '',
                'date_since': datetime.strftime(start, '%Y-%m-%d'),
                'date_until': datetime.strftime(end, '%Y-%m-%d')
            },
            'data': base64.b64encode('get_facebook'.encode('utf-8'))
        }

        get_facebook_data(evnt, '')

        start = end + timedelta(days=1)
