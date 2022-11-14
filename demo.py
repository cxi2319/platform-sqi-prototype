"""Demo Streamlit app for in-platform SQI
"""
import streamlit as st
from st_aggrid import AgGrid
from utils import loading, snowflake, processing

st.set_page_config(page_title="In-Platform SQI Prototype", page_icon="gear")
st.title("In-Platform SQI Prototype")

FILEPATH = "/Users/cxi/datascience/new-sqi-prototype/sqi_business_experience.csv"

st.markdown(
    """
    This app allows lets you pull search quality data from Snowflake by specifying filters such as business name, experience key and lookback period - without having to write a single SQL query.
    """
)
# Initialize user inputs in a sidebar
with st.form("user inputs"):
    with st.sidebar:
        # Number input for lookback period, maximum 30 days
        lookback = st.sidebar.number_input(
            "Lookback Period (Days)", min_value=0, max_value=30, value=14, step=1
        )
        st.sidebar.write("Table Inputs")
        # Load all options for business selection from a .csv file containing all businesses and experiences with an SQI score in October 2022
        businesses = loading.initialize_businesses(FILEPATH)
        # Business user input
        business_name = st.sidebar.selectbox("Business Name", options=businesses["BUSINESS_NAME"])
        # Load all experiences for a particular business from a .csv file containing all businesses and experiences with an SQI score in October 2022, given the business name the user inputted above
        experiences = loading.filter_experiences(businesses, business_name)
        # Fetch the business ID, used to grab the API key for connecting with the Yext Client
        user_business_id = loading.filter_businessid(businesses, business_name)
        # User input for experience
        experience_key = st.sidebar.selectbox(
            "Experience Key", options=experiences["EXPERIENCE_KEY"]
        )
        # Allow user to select the sort order of the table. Defaults to SQI ascending
        table_sort_order = st.selectbox(
            "Table Sort Order",
            options=["By SQI (Ascending)", "By Searches (Descending)"],
            help="Select a column to sort table by. Sort by either SQI (ascending) or searches (descending). Defaults to SQI ascending.",
        )
        # Allow user to select the minimum searches threshold for the table, using percentiles. This is to filter out long-tail queries. Defaults to 0.75
        st.sidebar.write("Select a Search Threshold")
        searches_percentile = st.slider(
            "Min. Searches Threshold",
            min_value=0.0,
            max_value=1.0,
            value=0.75,
            step=0.05,
            help="Select a minimum search volume threshold, represented by percentile of total searches, for queries to view SQI for.",
        )
        submitted = st.form_submit_button(label="Submit")

# Connect to Snowflake
st.experimental_singleton()


def connect_to_snowflake():
    return snowflake.connect_to_snowflake()


if submitted:
    tab1, tab2, tab3 = st.tabs(["Search Terms Table", "View Results", "Snowflake Query"])
    with tab1:
        CONN = connect_to_snowflake()
        # Load table containing all queries and SQI scores
        query_level_sqi = loading.query_level_sqi(business_name, experience_key, lookback, CONN)
        # Cleaning the dataframe of queries to display as a table
        # Drop unneccessary columns
        display_df = query_level_sqi.drop(
            columns=["business_name", "business_id", "experience_key", "monthly_experience_avg_sqi"]
        )
        # Establish a minimum searches threshold, based on the user inputted percentile and the search volume of the experience
        min_searches = int(
            round(processing.searches_floor(display_df, "total_searches", searches_percentile), 0)
        )
        # filter out any queries that do not meet the minimum searches threshold
        display_df = display_df[display_df["total_searches"] >= min_searches]
        # Sort the table based on the sort order selected by the user
        display_df = processing.sort_df(display_df, table_sort_order)
        # Hero number columns
        try:
            # Initialize columns for the hero numbers
            col1, col2, col3 = st.columns(3)
            # Average SQI for this experience for the given date range
            # Take the first value from the column since they're all the same
            experience_sqi = query_level_sqi["monthly_experience_avg_sqi"][0]
            col1.metric(
                "Avg. Experience SQI",
                value=experience_sqi,
                help="This is the average SQI score across this experience for the given lookback.",
            )
            # Get the global SQI (Average SQI across all experiences) for a given lookback period
            load_sqi = loading.global_sqi(lookback, CONN)
            # Take the first value from the column since they're all the same
            global_sqi = load_sqi["query_sqi_score"][0]
            col2.metric(
                "Global SQI average",
                value=global_sqi,
                help="This is the average SQI across every experience for the given lookback",
            )
            st.write(
                "Tip: click on the column headers to see additional sorting and filtering options!"
            )
            # Display the table containing queries, searches, SQI, and performance (above, at, or below average)
            AgGrid(display_df, height=700, theme="dark", fit_columns_on_grid_load=False)
        # If there is no data in the table, throw an IndexError to the user telling them to select a new date period
        except IndexError:
            st.write("No data found for this lookback period. Please select a different period.")
    with tab2:
        api_key = loading.get_api_key(user_business_id, CONN)
        client = loading.yextclient(api_key)
        searchterm = st.selectbox("Select a query:", options=display_df["query"])
        results = processing.return_response(client, searchterm, experience_key)
        # st.write(api_key)
        st.write(results)
    with tab3:
        # Raw Snowflake query
        raw_query = loading.return_query(loading.QUERY, business_name, experience_key, lookback)
        st.header("Snowflake Query")
        st.write("In case you wanted to query the data yourself!")
        with st.expander("View Query"):
            st.code(raw_query, language="sql")
