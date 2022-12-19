import numpy as np

# Return minimum number of searches for an experience given a percentile threshold
def searches_floor(df, col, quantile):
    searches_floor = df[col].quantile(quantile)
    return searches_floor


# Sort the display dataframe
def sort_df(df, sort_index):
    if sort_index == "By SQI (Ascending)":
        # sort by SQI descending by default
        df_sorted = df.sort_values(by=["query_sqi_score"])
    elif sort_index == "By Searches (Descending)":
        df_sorted = df.sort_values(by=["total_searches"], ascending=False)
    return df_sorted


# Given an experience key and query, fetch a result set from the Yext client to preview results and return as a list of dicts.
def return_raw_response(client, query, experience_key):
    # Get raw results from the YextClient
    raw_results = client.search_answers_universal(f"{query}", f"{experience_key}")
    # Get the raw results dictionary attribute from the search_answers_universal class
    results_dict = raw_results.__dict__
    return results_dict


def cleaned_response(dict):
    # Only grab the API response from the raw results attribute
    response_dict = dict["response"]
    # Remove the business ID from the response
    response_dict.pop("businessId")
    # Compile the reponse into a list of dictionaries, each dictionary containing a vertical response
    response_list = response_dict["modules"]
    # return raw response list of dicts
    return response_list


# Function to get entity profile fileds for KG results. Returns a list
def get_all_fields(list):
    # Initialize empty list of fields
    fields = []
    # Get the dictionary containing entity profiles
    dict = list[0]
    # Iterate through entity profile fields
    for key in dict:
        # Check to see if the field contains the entity data
        if key == "data":
            result = dict[key]
            # Append each entity profile field to the list of fields
            for x in result:
                fields.append(x)
    return fields


# Function to get entity profile fields for third-party Links results. Returns a list
def get_link_fields(list):
    # Initialize empty list of fields
    fields = []
    # Iterate through each field in the Links profile
    for key in list[0]:
        fields.append(key)
    return fields


# Function to extract all of the unique profile fields from a list of entity profile fields for a given result
def unique_fields(list):
    x = np.array(list)
    unique_list = np.unique(x)
    return unique_list


def get_card_display(profile: dict, fields: list[str], char_lim: int = 500):
    """
    Renders the fields from the profile of an entity. Applies a limit on the value.
    """
    display = ""
    first_field = True
    for field in fields:
        value = profile.get(field, "")
        value = str(value)

        if char_lim and len(value) > char_lim:
            value = value[:char_lim] + " ..."

        if first_field:
            display += f"### {value}\n"
            first_field = False
        else:
            display += f"{value}\n\n"

    return display
