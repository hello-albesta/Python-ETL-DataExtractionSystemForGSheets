# Python-ETL-DataExtractionSystemForGSheets
This repository houses my project for a data extraction system using Apache Airflow. The system follows a workflow and goes through multiple stages. Initially, data is extracted from PostgreSQL and then filtered to handle empty and duplicate data rows. Then, the format is adjusted and sent to Google Sheets daily at 2:00 AM for each day via API.
