import pandas as pd
import streamlit as st
from yext import YextClient
from utils import snowflake

QUERY = """
    select
        distinct searches.tokenizer_normalized_query as query,
        yext_accounts.business_id,
        yext_accounts.business_name,
        searches.experience_key,
        agg_sqi_v2_by_experience.sqi as monthly_experience_avg_sqi,
        avg(agg_sqi_v2_results.sqi) as query_sqi_score,
        count(tokenizer_normalized_query) as total_searches,
        case
            when round(avg(agg_sqi_v2_results.sqi),2) < round(monthly_experience_avg_sqi,2) then 'Below Average'
            when round(avg(agg_sqi_v2_results.sqi),2) = round(monthly_experience_avg_sqi,2) then 'Average'
            else 'Above Average' end as performance
    from prod_data_science.public.agg_sqi_v2_results
    join prod_data_hub.answers.searches using(query_id)
    join prod_product.public.yext_accounts using (business_id)
    join prod_data_science.public.agg_sqi_v2_by_experience
        on yext_accounts.business_id = agg_sqi_v2_by_experience.business_id
        and searches.experience_key = agg_sqi_v2_by_experience.experience_key
        and year(searches.timestamp) = agg_sqi_v2_by_experience.year
        and month(searches.timestamp) = agg_sqi_v2_by_experience.month
    where date(searches.timestamp) >= dateadd('day', -{}, current_date())
    and yext_accounts.business_name = '{}'
    and searches.experience_key = '{}'
    group by 1, 2, 3, 4, 5
    order by 7 desc
    """

BUSINESS_QUERY = """
    select
        avg(agg_sqi_v2_results.sqi) as query_sqi_score
    from prod_data_science.public.agg_sqi_v2_results
    join prod_data_hub.answers.searches using(query_id)
    join prod_product.public.yext_accounts using (business_id)
    join prod_data_science.public.agg_sqi_v2_by_experience
        on yext_accounts.business_id = agg_sqi_v2_by_experience.business_id
        and searches.experience_key = agg_sqi_v2_by_experience.experience_key
        and year(searches.timestamp) = agg_sqi_v2_by_experience.year
        and month(searches.timestamp) = agg_sqi_v2_by_experience.month
    where date(searches.timestamp) >= dateadd('day', -{}, current_date())
"""
# Load available businesses + experiences
def initialize_businesses(filepath):
    business_df = pd.read_csv(filepath)
    business_df = business_df.rename(str.lower, axis="columns")
    return business_df


# Filter down to experiences that correspond to a given business once a user selects one
def filter_experiences(df, business):
    df_filtered = df[df["business_name"] == business]
    return df_filtered


def filter_businessid(df, business):
    business_id = df.loc[df["business_name"] == business, "business_id"].values[0]
    return business_id


# Return SQI search term dataframe
@st.experimental_memo()
def query_level_sqi(business, experience, lookback, _conn):
    query = QUERY.format(
        lookback,
        business,
        experience,
    )
    df = snowflake.get_data_from_snowflake(query, _conn)
    # Round column values to nearest tenth
    df["monthly_experience_avg_sqi"] = df["monthly_experience_avg_sqi"].round(2)
    df["query_sqi_score"] = df["query_sqi_score"].round(2)
    return df


# Load a dataframe containing the global SQI for a given lookback period
def global_sqi(lookback, conn):
    query = BUSINESS_QUERY.format(lookback)
    result = snowflake.get_data_from_snowflake(query, conn)
    result = round(result, 2)
    return result


# Fetch a given business's Search API key
def get_api_key(business_id, conn):
    query = """
            select api_key
            from prod_product.public.search_api_keys
            where business_id = {}
            limit 1
        """.format(
        business_id
    )
    key = snowflake.get_data_from_snowflake(query, conn)
    # Check to see if the API key exists
    if len(key.index) == 0:
        raise ValueError("No API Key found for selected business.")
    api_key = key["api_key"][0]
    return api_key


# Connect to the Yext client
def yextclient(api_key):
    return YextClient(api_key)


# Return raw data query
def return_query(query, business, experience, lookback):
    query = query.format(
        lookback,
        business,
        experience,
    )
    return query
