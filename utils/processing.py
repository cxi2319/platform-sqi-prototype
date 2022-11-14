def searches_floor(df, col, quantile):
    percentile = df[col].quantile(quantile)
    return percentile


def sort_df(df, sort_index):
    if sort_index == "By SQI (Ascending)":
        # sort by SQI descending by default
        df_sorted = df.sort_values(by=["query_sqi_score"])
    elif sort_index == "By Searches (Descending)":
        df_sorted = df.sort_values(by=["total_searches"], ascending=False)
    return df_sorted


def return_response(client, query, experience_key):
    results_dict = client.search_answers_universal(f"{query}", f"{experience_key}")
    # return response
    return results_dict
