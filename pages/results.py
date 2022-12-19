import streamlit as st
from yext import YextClient
from utils import loading, snowflake, processing

st.set_page_config(page_title="View Search Results", page_icon="gear")
st.title("In-Platform SQI")

FILEPATH = "/Users/cxi/datascience/new-sqi-prototype/sqi_business_experience.csv"

st.markdown(
    """
    This app allows lets you pull search quality data from Snowflake by specifying filters such as business name, experience key and lookback period - without having to write a single SQL query.
    """
)
# Initialize user inputs in a sidebar
with st.sidebar:
    # Number input for lookback period, maximum 30 days
    st.write("Select a lookback period")
    lookback = st.sidebar.number_input(
        "Lookback Period (Days)",
        min_value=0,
        max_value=30,
        value=14,
        step=1,
        help="Maximum 30 days",
    )
    st.sidebar.write("Select a business and experience")
    # Load all options for business selection from a .csv file containing all businesses and experiences with an SQI score in October 2022
    businesses = loading.initialize_businesses(FILEPATH)
    unique_businesses = businesses.business_name.unique()
    # Business user input
    business_name = st.sidebar.selectbox("Business Name", options=unique_businesses)
    # Fetch the business ID, used to grab the API key for connecting with the Yext Client
    user_business_id = loading.filter_businessid(businesses, business_name)
    # Load all experiences for a particular business from a .csv file containing all businesses and experiences with an SQI score in October 2022, given the business name the user inputted above
    experiences = loading.filter_experiences(businesses, business_name)
    unique_experiences = experiences.experience_key.unique()
    # User input for experience
    experience_key = st.sidebar.selectbox("Experience Key", options=experiences["experience_key"])
    # Allow user to select the minimum searches threshold for the table, using percentiles. This is to filter out long-tail queries. Defaults to 0.99
    st.sidebar.write("Select a search threshold")
    searches_percentile = st.slider(
        "Min. Searches Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.99,
        step=0.01,
        help="Select a minimum search volume threshold, represented by percentile of total searches, for queries to view SQI for.",
    )

# Connect to Snowflake
@st.experimental_singleton()
def connect_to_snowflake():
    return snowflake.connect_to_snowflake()


CONN = connect_to_snowflake()

# Load table containing all queries and SQI scores
query_level_sqi = loading.query_level_sqi(business_name, experience_key, lookback, CONN)
# Query the dataframe so that we have a list of searches
# Drop unneccessary columns
display_df = query_level_sqi.drop(
    columns=["business_name", "business_id", "experience_key", "avg_experience_sqi"]
)
display_df = display_df.reset_index(drop=True)
# Establish a minimum searches threshold, based on the user inputted percentile and the search volume of the experience
min_searches = int(
    round(processing.searches_floor(display_df, "total_searches", searches_percentile), 0)
)
# filter out any queries that do not meet the minimum searches threshold
display_df = display_df[display_df["total_searches"] >= min_searches]
# Sort the table based on the sort order selected by the user
display_df = processing.sort_df(display_df, sort_index="By SQI (Ascending)")


st.header("View Search Results")

# Function to fetch the API key to connect to the YextClient
@st.experimental_memo()
def get_api_key(business_id, _conn):
    query = """
            select api_key
            from prod_product.public.search_api_keys
            where business_id = {}
            limit 1
        """.format(
        business_id
    )
    key = snowflake.get_data_from_snowflake(query, _conn)
    # Check to see if the API key exists
    if len(key.index) == 0:
        raise ValueError("No API Key found for selected business.")
    api_key = key["api_key"][0]
    return api_key


# Fetch the API key to connect to the YextClient
api_key = get_api_key(user_business_id, CONN)

# Function to connect to the YextClient
@st.experimental_memo()
def yextclient(api_key):
    return YextClient(api_key)


# Connect to the YextClient
client = yextclient(api_key)
# Selectbox for the user to select a query
query_select = st.selectbox("Select a query:", options=display_df["query"])
# Fetch the raw response object from the YextClient for the query
raw_response = processing.return_raw_response(client, query_select, experience_key)
# Clean the response, remove unecessary parameters
response = processing.cleaned_response(raw_response)
# Initialize empty list of field params, for the user to select as display fields
fields_list = []
# Iterate through each vertical in the response
for item in response:
    # Iterate through each param of the vertical response
    for key in item:
        # Check to see if the param contains the vertical title
        if key == "verticalConfigId":
            vertical = item.get(key).title()
        # Check to see if the param contains the vertical results object
        if key == "results":
            # Get result entities
            result_list = item.get(key)
            # If the vertical is a Knowledge Graph vertical, append all fields from the vertical's entities to the list of display fields
            if vertical != "Links":
                fields_list.append(processing.get_all_fields(result_list))
            # If it's a third-party links vertical create a list and append all the fields to that list
            else:
                link_list = processing.get_link_fields(result_list)
# Check to see if there were no display fields, in that case there were no KG results
if fields_list == []:
    st.subheader("No Knowledge Graph results found for this query.")
# flatten the list of all display fields for all verticals in a result set
flat_list = [item for sublist in fields_list for item in sublist]
# Only get the unique fields in the flattened list to display
unique_list = processing.unique_fields(flat_list)
# Initialize sidebar select option for display fields to display on KG entity profiles
st.sidebar.write("Select entity fields to display on KG result profiles")
try:
    display_fields = st.sidebar.multiselect(
        "Display Fields",
        unique_list,
        default=["name"] if "name" in unique_list else [],
    )
except NameError:
    st.sidebar.text_input(
        "Display Fields",
        label="No Knowledge Graph results were found for this query.",
        disabled=True,
    )
# Initialize sidebar select option for display fields to display on third-party links entity profiles
st.sidebar.write("Select fields to display on Links result profiles")
try:
    links_fields = st.sidebar.multiselect(
        "Links Fields",
        link_list,
        default=["htmlTitle"] if "htmlTitle" in link_list else [],
    )
except NameError:
    st.sidebar.text_input(
        "Links Fields",
        value="No Third-Party Links results were found for this query",
        disabled=True,
    )
# Provide link to search log
query_id = raw_response.get("query_id")
search_log_url = f"[View Search Log](https://www.yext.com/s/{user_business_id}/search/experiences/{experience_key}/searchQueryLogDetails/{query_id})"
st.markdown(search_log_url, unsafe_allow_html=True)
# Display hero numbers to display for the query
try:
    # Initialize columns for the hero numbers
    col1, col2, col3 = st.columns(3)
    # Average SQI for the query
    query_sqi = display_df.loc[display_df["query"] == query_select, "query_sqi_score"]
    col1.metric(
        f"SQI score",
        value=query_sqi,
        help="This is the average SQI score for this query.",
    )
    # The total number of searches for the query
    query_searches = display_df.loc[display_df["query"] == query_select, "total_searches"]
    col2.metric(
        "Count of searches",
        value=query_searches,
        help="This is the total number of searches for this query.",
    )
    # Performance label for the query
    query_performance = display_df.loc[display_df["query"] == query_select, "performance"].values[0]
    col3.metric(
        "Query Performance",
        value=query_performance,
        help="Whether the query performed above, below, or at average relative to all queries in the experience.",
    )
# If there is no data in the table, throw an IndexError to the user telling them to select a new date period
except IndexError:
    st.write("No data found for this lookback period. Please select a different period.")

# Display the result cards for the query
# Iterate through each vertical in the response
for item in response:
    # Iterate through each param of the vertical response
    for key in item:
        # Check to see if the param contains the vertical title
        if key == "verticalConfigId":
            vertical = item.get(key).title()
            st.subheader("Vertical: " + vertical)
        # Check to see if the param contains the vertical results object
        if key == "results":
            # Iterate through each entity in the vertical results
            for object in item.get(key):
                # Iterate through each field in the entity profile
                for field in object:
                    # Check if the Vertical is a links vertical
                    if vertical == "Links":
                        link = object
                    else:
                        # Get each field for non-links verticals
                        if field == "data":
                            result = object.get(field)
                # Display Entity Profiles
                if vertical == "Links":
                    st.info(processing.get_card_display(link, links_fields))
                else:
                    st.info(processing.get_card_display(result, display_fields))
# Render full raw API response for the user to view
st.header("View Full API Response")
with st.expander("Click to view"):
    st.write(raw_response["raw_response"])
