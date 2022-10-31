import pandas as pd
from datetime import date
from utils import snowflake


def initialize_businesses(filepath):
    business_df = pd.read_csv(filepath)
    return business_df


def filter_experiences(df, business):
    df_filtered = df[df["BUSINESS_NAME"] == business]
    return df_filtered


def query_level_sqi(business, experience, lookback, conn):
    query = """
                select
                    distinct searches.tokenizer_normalized_query as query,
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
                group by 1, 2, 3, 4
                order by 6 desc
    """.format(
        lookback,
        business,
        experience,
    )
    df = snowflake.get_data_from_snowflake(query, conn)
    df["monthly_experience_avg_sqi"] = df["monthly_experience_avg_sqi"].round(2)
    df["query_sqi_score"] = df["query_sqi_score"].round(2)
    return df
