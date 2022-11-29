import streamlit as st
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
# with st.form("user inputs"):
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
    # Allow user to select the sort order of the table. Defaults to SQI ascending
    table_sort_order = st.selectbox(
        "Table Sort Order",
        options=["By SQI (Ascending)", "By Searches (Descending)"],
        help="Select a column to sort table by. Sort by either SQI (ascending) or searches (descending). Defaults to SQI ascending.",
    )
    # Allow user to select the minimum searches threshold for the table, using percentiles. This is to filter out long-tail queries. Defaults to 0.90
    st.sidebar.write("Select a search threshold")
    searches_percentile = st.slider(
        "Min. Searches Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.90,
        step=0.05,
        help="Select a minimum search volume threshold, represented by percentile of total searches, for queries to view SQI for.",
    )
    # submitted = st.form_submit_button(label="Submit")

# Connect to Snowflake
@st.experimental_singleton()
def connect_to_snowflake():
    return snowflake.connect_to_snowflake()


CONN = connect_to_snowflake()

# Load table containing all queries and SQI scores
query_level_sqi = loading.query_level_sqi(business_name, experience_key, lookback, CONN)
# Cleaning the dataframe of queries to display as a table
# Drop unneccessary columns
display_df = query_level_sqi.drop(
    columns=["business_name", "business_id", "experience_key", "monthly_experience_avg_sqi"]
)
display_df = display_df.reset_index(drop=True)
# Establish a minimum searches threshold, based on the user inputted percentile and the search volume of the experience
min_searches = int(
    round(processing.searches_floor(display_df, "total_searches", searches_percentile), 0)
)
# filter out any queries that do not meet the minimum searches threshold
display_df = display_df[display_df["total_searches"] >= min_searches]
# Sort the table based on the sort order selected by the user
display_df = processing.sort_df(display_df, table_sort_order)


st.header("View Search Results")
st.write("Select a query to preview its result set. You can also view the full API response.")
api_key = loading.get_api_key(user_business_id, CONN)
client = loading.yextclient(api_key)
if "query" not in st.session_state:
    st.session_state.query = display_df["query"].iloc[0]
existing = st.session_state.query
values = display_df["query"].values.tolist()
# st.write(display_df["query"])
st.write("Current session state is:", st.session_state)
query_select = st.selectbox(
    "Select a query:",
    options=display_df["query"],
    key="query",
    help="Sometimes the query will reset itself, so re-select the search",
)
st.write("Viewing result set for", st.session_state.query)
st.write("Now the session state is:", st.session_state)
raw_response = processing.return_raw_response(client, st.session_state, experience_key)
response = processing.cleaned_response(raw_response)
fields_list = []
for item in response:
    for key in item:
        if key == "verticalConfigId":
            vertical = item.get(key).title()
        if key == "results":
            result_list = item.get(key)
            if vertical != "Links":
                fields_list.append(processing.get_all_fields(result_list))
            else:
                link_list = processing.get_link_fields(result_list)
if fields_list == []:
    st.subheader("No results found for this query.")
flat_list = [item for sublist in fields_list for item in sublist]
unique_list = processing.unique_fields(flat_list)
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
        label="No KG results were found for this query.",
        disabled=True,
    )
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
        value="No Links results were found for this query",
        disabled=True,
    )
for item in response:
    for key in item:
        if key == "verticalConfigId":
            vertical = item.get(key).title()
            st.subheader("Vertical: " + vertical)
        if key == "results":
            for object in item.get(key):
                for field in object:
                    if vertical == "Links":
                        link = object
                    else:
                        if field == "data":
                            result = object.get(field)
                if vertical == "Links":
                    st.info(processing.get_card_display(link, links_fields))
                else:
                    st.info(processing.get_card_display(result, display_fields))
# Render full raw API response for the user to view
st.header("View Full API Response")
with st.expander("Click to view"):
    st.write(raw_response["raw_response"])
