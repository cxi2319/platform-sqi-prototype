# platform-sqi-prototype

### Prototype for Search Quality app ###
The purpose of this app is to allow a user to fetch search quality data for a given business, from Snowflake. There are two components to this app. The first is to visualize the SQI scores for each individual query in table format. The second is to select a given query and visualize a real result set for that query, to assess the quality of the prediction being served by the SQI model.

A user can specify a lookback period, business ID, and experience key. Once they do this, they can optionally select a search threshold, to filter out long tail search queries. 



