"""Demo Streamlit app for in-platform SQI
"""
import streamlit as st
from st_aggrid import GridOptionsBuilder, AgGrid
from utils import loading, snowflake

st.set_page_config(page_title="In-Platform SQI Prototype", page_icon="gear")
st.title("In-Platform SQI Prototype")

FILEPATH = "/Users/cxi/datascience/sqi_prototype/sqi_business_experience.csv"
LOOKBACK = st.sidebar.number_input(
    "Lookback Period (Days)", min_value=0, max_value=30, value=14, step=1
)

st.markdown(
    """
    This app allows lets you pull search quality data from Snowflake by specifying filters such as business name, experience key and lookback period - without having to write a single SQL query.
    """
)
# Initialize user inputs for business and experience key
with st.form("user inputs"):
    with st.sidebar:
        st.sidebar.write("User Inputs")
        businesses = loading.initialize_businesses(FILEPATH)
        business_name = st.sidebar.selectbox("Business Name", options=businesses["BUSINESS_NAME"])
        experiences = loading.filter_experiences(businesses, business_name)
        experience_key = st.sidebar.selectbox(
            "Experience Key", options=experiences["EXPERIENCE_KEY"]
        )
        submitted = st.form_submit_button(label="Submit")

# Connect to Snowflake
st.experimental_singleton()


def connect_to_snowflake():
    return snowflake.connect_to_snowflake()


if submitted:
    CONN = connect_to_snowflake()
    # Load table containing all queries and SQI scores
    query_level_sqi = loading.query_level_sqi(business_name, experience_key, LOOKBACK, CONN)
    display_df = query_level_sqi.drop(
        columns=["business_name", "experience_key", "monthly_experience_avg_sqi"]
    )
    # Hero number columns
    # Experience-level SQI - just take the first value from the column since they're all the same
    try:
        experience_sqi = query_level_sqi["monthly_experience_avg_sqi"][0]
        st.metric(
            "Avg. Experience SQI",
            value=experience_sqi,
            help="This is the average SQI score across this experience.",
        )
        col1, col2, col3, col4 = st.columns(4)
        # Best-performing query by SQI
        best_sqi_query = query_level_sqi["query_sqi_score"].max()
        col1.metric(
            "Best-performing SQI",
            value=best_sqi_query,
            help="This is the best SQI score across this experience.",
        )
    except IndexError:
        st.write("No data found for this lookback period. Please select a different period.")
    AgGrid(display_df, height=700, theme="dark", fit_columns_on_grid_load=False)
