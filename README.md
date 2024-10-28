# **Google Trends Data Extraction**

This project automates the daily extraction, processing, 
and storage of Google Trends data for various programming 
languages across regions. Using Airflow, it retrieves trend data 
via the pytrends API, structures it in a DataFrame,
and stores it in a PostgreSQL database. 
The Airflow DAG handles data retrieval, dynamic table creation, 
and data insertion, ensuring a regularly updated dataset for 
trend analysis.