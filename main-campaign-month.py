import json
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
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

schema_exchange_rate = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("currencies", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rate", "FLOAT", mode="REQUIRED")
]

schema_facebook_stat = [
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

    resp = client.insert_rows_json(
        json_rows=data,
        table=table_ref,
    )

    print(resp)

    logger.info("Success uploaded to table {}".format(table.table_id))


def wait_for_async_job(job):
    job.api_get()
    i = 0
    while job[AdReportRun.Field.async_status] != "Job Completed":
        time.sleep(10)
        i = i + 1
        print(f'{i * 10}s: Fetching Facebook Data...')
        job.api_get()
    time.sleep(10)
    return job.get_result(params={"limit": 1000})


def get_facebook_data(event, context):

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    bigquery_client = bigquery.Client()

    if 'date_until' in event['attributes']:
        date_until = datetime.strptime(
            event['attributes']['date_until'], '%Y-%m-%d')
    else:
        date_until = date.today() - timedelta(1)

    if 'date_since' in event['attributes']:
        date_since = datetime.strptime(
            event['attributes']['date_since'], '%Y-%m-%d')
    else:
        date_since = date.today() - timedelta(1)

    if pubsub_message == 'get_currency':

        table_id = event['attributes']['table_id']
        dataset_id = event['attributes']['dataset_id']
        project_id = event['attributes']['project_id']

        api_key = event['attributes']['api_key']
        from_currency = event['attributes']['from_currency']
        to_currency = event['attributes']['to_currency']
        source = from_currency+to_currency

        cur_source = []

        params = {'access_key': api_key,
                  'currencies': to_currency,
                  'source': from_currency,
                  'date': date_until.strftime("%Y-%m-%d")
                  }

        url = 'http://api.currencylayer.com/historical'

        try:
            r = requests.get(url, params=params)
        except requests.exceptions.RequestException as e:
            logger.error('request to currencylayer error: {}').format(e)
            return e

        if r.json()["success"] is True:

            exist_dataset_table(bigquery_client, table_id,
                                dataset_id, project_id, schema_exchange_rate)

            cur_source.append({'date': date_until.strftime("%Y-%m-%d"),
                               'currencies': source,
                               'rate': r.json()['quotes'][source]
                               })

            insert_rows_bq(bigquery_client, table_id,
                           dataset_id, project_id, cur_source)
        else:
            logger.error('request to currencylayer error: {}').format(
                r.json()['error']['info'])

        return 'ok'

    elif pubsub_message == 'get_facebook':

        table_id = event['attributes']['table_id']
        dataset_id = event['attributes']['dataset_id']
        project_id = event['attributes']['project_id']

        app_id = event['attributes']['app_id']
        app_secret = event['attributes']['app_secret']
        access_token = event['attributes']['access_token']
        account_id = event['attributes']['account_id']

        try:
            FacebookAdsApi.init(app_id, app_secret,
                                access_token, api_version='v13.0')

            account = AdAccount('act_'+str(account_id))
            job = account.get_insights_async(fields=[
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
            ], params={
                'level': 'campaign',
                'time_range': {
                    'since':  date_since.strftime("%Y-%m-%d"),
                    'until': date_until.strftime("%Y-%m-%d")
                }, 'time_increment': 'monthly'
            })

            result_cursor = wait_for_async_job(job)
            insights = [item for item in result_cursor]

            df = pandas.DataFrame(insights)

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

            for item in array_fields:
                if item in df.columns:
                    df[item] = df[item].fillna('').apply(list)
                else:
                    df[item] = df.apply(lambda _: [], axis=1)

            # df.to_csv('fb_data.csv', index=None)

        except Exception as e:
            logger.info(e)
            print(e)
            raise

        if exist_dataset_table(bigquery_client, table_id, dataset_id, project_id, schema_facebook_stat, clustering_fields_facebook) == 'ok':

            print('Inserting data to bigquery...')
            insert_rows_bq(bigquery_client, table_id,
                           dataset_id, project_id, json.loads(df.to_json(orient='records')))
            print('Done inserting...')

            return 'ok'


evnt = {
    'attributes': {
        'table_id': 'raw_data_campaign_monthly',
        'dataset_id': 'facebook_marketing_api',
        'project_id': 'bitburger-marketing',
        'app_id': '2203459236486852',
        'app_secret': '0eb6102afa89aec892d460217d687abe',
        'access_token': 'EAAfUCNURXsQBACybU8woTF4dNRFZAUfe2cuL8ZAGoZCq4Tv7Un4Yve0ZAQhVpfGbysZCajl5GGZA94DdZA1rt0auqRWXVcngaJrRvCxE4HtEBevnBBtbrWzVre3XzV2KcPGaVmvFPBr8FQlLN9DZCHxYUtDS2ttGE4YtkWIgtFF2gtsu9wZBu7ZB3M',
        'account_id': '258501552275269',
        'date_since': '2019-05-01',
        'date_until': '2022-04-30'
    },
    'data': base64.b64encode('get_facebook'.encode('utf-8'))
}

get_facebook_data(evnt, '')


def getIntervalFacebookData(start: str, limit: int, interval: int):
    i = 0
    yesterday = datetime.today() - timedelta(days=1)

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
                'project_id': 'bitburger-marketing',
                'app_id': '625092038584719',
                'app_secret': '41ab12354adccbade22f9bd9abf2491b',
                'access_token': 'EAANnwJEZCed8BAEZBjXpIxMtTeDfwp40nuMgly1ZAsxw0hMudAnhH9Bup7mDdoUas2Sg6CZB3klJpRuGmjW89ZApkki8ZAs2UOOOflBiFhcpmySZANsfxvO4djqucV2hzFl8LXXZBXqYabPCEzXRi4ndaZBAHqhihp6a4mAKIYkhtozX0eHfPoJrm',
                'account_id': '234370047369717',
                'date_since': datetime.strftime(start, '%Y-%m-%d'),
                'date_until': datetime.strftime(end, '%Y-%m-%d')
            },
            'data': base64.b64encode('get_facebook'.encode('utf-8'))
        }

        get_facebook_data(evnt, '')

        start = end + timedelta(days=1)

# getIntervalFacebookData('2022-04-29', 1, 1)
