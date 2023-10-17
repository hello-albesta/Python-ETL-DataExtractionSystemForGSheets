"""
    All List of modules needed for make the programs works
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.models import Variable
from pytz import timezone

default_args = {
    'owner': 'airflow_albesta',
    'start_date': timezone("Asia/Jakarta").localize(
        datetime(
            year=2023,
            month=9,
            day=5,
            hour=3),
        is_dst=None
    ),
    'retries': 1,
}

with (DAG(
        dag_id='chinook-dag.2023_albesta_ver1.0',
        default_args=default_args,
        schedule_interval='0 2 * * *',
        catchup=False
)):
    import decimal

    import json
    import pandas as pd
    import numpy as np

    CONST_CREATOR_NAME = "Daniel Albesta"

    CONST_POSTGRES_CONN_ID = "postgres_conn_id"
    CONST_GCP_CONN_ID = "gcp_conn_id"

    CONST_GSHEET_FILE_SHEET_ID = "gsheet_file_sheet_id"
    CONST_GSHEET_FILE_LIST_SHEET = "gsheet_file_list_sheets"
    CONST_GSHEET_MAJOR_DIMENSION_OPTION = "ROWS"
    CONST_GSHEET_VALUE_INPUT_OPTION = "USER_ENTERED"
    CONST_GSHEET_INSERT_DATA_OPTION = "INSERT_ROWS"

    CONST_STAGING_SHEET = "Staging_sheet"
    CONST_PROD_SHEET = "Prod_sheet"

    CONST_FORMATTED_VALUE = "FORMATTED_VALUE"


    class TransformData:
        """ Class for transforming data. """

        @staticmethod
        def handle_file_content_null_value(resource_data):
            """
                This static method handles null values in the given
                resource data.

                Parameters:
                    resource_data (pandas.DataFrame): The resource
                    data to handle the null values for.

                Returns:
                    pandas.DataFrame: The resource data with null
                    values removed.
            """

            data_row_null_flag = len(resource_data[resource_data.isnull().any(axis=1)])

            resource_data = resource_data.dropna().reset_index(drop=True)

            print(f"HAS BEEN DELETED: {data_row_null_flag} ROWS CONTAINS NULL VALUES")

            return resource_data

        @staticmethod
        def handle_file_data_row_duplicate(resource_data):
            """
                Removes duplicate rows from the given resource_data
                DataFrame and returns the modified DataFrame.

                Parameters:
                    resource_data (pandas.DataFrame): The input
                    DataFrame containing the resource data.

                Returns:
                    pandas.DataFrame: The modified DataFrame
                    after removing duplicate rows.
            """

            data_row_duplicate_flag = len(resource_data[resource_data.duplicated()])

            resource_data = resource_data.drop_duplicates().reset_index(drop=True)

            print(f"HAS BEEN DELETED: {data_row_duplicate_flag} ROWS CONTAINS DUPLICATE VALUES")

            return resource_data


    class HandleInt64DecimalValueEncoder(json.JSONEncoder):
        """ Class for transforming data. """

        def default(self, o):
            """
                Convert the given object to a serializable format.

                Parameters:
                    o (object): The object to be converted.

                Returns:
                    The converted object in a serializable format.
            """

            if isinstance(o,
                          np.integer):
                return int(o)

            if isinstance(o,
                          decimal.Decimal):
                return float(o)

            if isinstance(o,
                          datetime):
                return o.astimezone(timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H-%M-%S")

            return json.JSONEncoder.default(self, o)


    def etl_sql_data_to_df():
        """
            Retrieves data from an SQL database and transforms it
            into a Panda's DataFrame.

            Returns:
                pandas.DataFrame: The transformed data as a
                DataFrame.
        """

        res_postgres_conn_id = Variable.get(CONST_POSTGRES_CONN_ID)

        postgres_hook = PostgresHook(postgres_conn_id=res_postgres_conn_id)

        postgres_query = """
            SELECT
                TRK.id AS "TrackId",
                TRK.name AS "Track",
                TRK.name AS "Name",
                ALB.title AS "AlbumName",
                ART.name AS "ArtisName",
                TRK.media_type_id AS "MediaType",
                TRK.genre_id AS "GenreId",
                TRK.composer AS "Composer",
                TRK.milliseconds AS "Miliseconds",
                TRK.bytes AS "Bytes",
                TRK.unit_price AS "UnitPrice",
                TO_CHAR((TRK.milliseconds || ' millisecond')::INTERVAL, 'MI') :: NUMERIC +
                        ROUND(((TO_CHAR((TRK.milliseconds || ' millisecond')::INTERVAL, '.SSMS') 
                        :: NUMERIC) / 60 ) * 100, 2) 
                        AS "DurationMinutes"

            FROM
                public.artists AS ART
    
                INNER JOIN public.albums AS ALB
                    ON ART.id = ALB.artist_id

                INNER JOIN public.tracks AS TRK
                    ON ALB.id = TRK.album_id

            ORDER BY
                TRK.id ASC
        """

        etl_data = pd.DataFrame(postgres_hook.get_records(sql=postgres_query))

        etl_data = TransformData.handle_file_content_null_value(
            resource_data=etl_data
        )

        etl_data = TransformData.handle_file_data_row_duplicate(
            resource_data=etl_data
        )

        return etl_data

    def convert_df_to_list_data(df_data):
        """
            Convert a panda's DataFrame to a list data structure.

            Parameters:
                df_data (pandas.DataFrame): The DataFrame to be
                converted.

            Returns:
                list: The converted list data structure.
        """

        list_data = df_data.values.tolist()

        return list_data

    def etl_sql_to_gsheet_data(interval_process_seq):
        """
            ETLs SQL data to Google Sheets data.

            Args:
                interval_process_seq (int): The sequence number of
                the interval process.

            Returns:
                None
        """

        etl_data = etl_sql_data_to_df()
        etl_data = convert_df_to_list_data(df_data=etl_data)

        for etl_data_idx, _ in enumerate(etl_data):
            if etl_data_idx == interval_process_seq:
                etl_data[etl_data_idx].extend([datetime.now(), CONST_CREATOR_NAME])
                etl_data = [etl_data[etl_data_idx]]

        json_dec_value_encoder = json.dumps(obj=etl_data,
                                            cls=HandleInt64DecimalValueEncoder)
        etl_data = json.loads(json_dec_value_encoder)

        print(f"final element {etl_data}")

        res_gcp_conn_id = Variable.get(CONST_GCP_CONN_ID)
        res_gsheet_file_sheet_id = Variable.get(CONST_GSHEET_FILE_SHEET_ID)

        res_gsheet_file_list_sheets = Variable.get(CONST_GSHEET_FILE_LIST_SHEET)
        res_gsheet_file_list_sheets = json.loads(res_gsheet_file_list_sheets)

        export_gsheet_file_staging_sheets = res_gsheet_file_list_sheets[CONST_STAGING_SHEET]
        export_gsheet_file_prod_sheets = res_gsheet_file_list_sheets[CONST_PROD_SHEET]

        gsheet_file_spreadsheet_hook = GSheetsHook(gcp_conn_id=res_gcp_conn_id)

        gsheet_file_spreadsheet_hook.append_values(
            spreadsheet_id=res_gsheet_file_sheet_id,
            range_=export_gsheet_file_staging_sheets,
            values=etl_data,
            major_dimension=CONST_GSHEET_MAJOR_DIMENSION_OPTION,
            value_input_option=CONST_GSHEET_VALUE_INPUT_OPTION,
            insert_data_option=CONST_GSHEET_INSERT_DATA_OPTION
        )

        gsheet_file_spreadsheet_hook.append_values(
            spreadsheet_id=res_gsheet_file_sheet_id,
            range_=export_gsheet_file_prod_sheets,
            values=etl_data,
            major_dimension=CONST_GSHEET_MAJOR_DIMENSION_OPTION,
            value_input_option=CONST_GSHEET_VALUE_INPUT_OPTION,
            insert_data_option=CONST_GSHEET_INSERT_DATA_OPTION
        )

    def etl_process_check_date():
        """
            Check the date for the ETL process.

            This function retrieves the initial date process from
            the default arguments and the current date in the
            'Asia/Jakarta' timezone. It then calculates the
            difference between the current date and the initial
            date process. The difference is converted to the number
            of days using the total_seconds() method. Finally, the
            function calls the etl_sql_to_gsheet_data() function
            with the calculated interval as the parameter.

            Parameters:
                'None'

            Returns:
                None
        """

        init_date_process = default_args.get('start_date').strftime("%Y-%m-%d %H")
        init_date_process = datetime.strptime(init_date_process,
                                              '%Y-%m-%d %H')

        scheduler_date_process = datetime.now(timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H")
        scheduler_date_process = datetime.strptime(scheduler_date_process,
                                                   '%Y-%m-%d %H')

        between_date_process = scheduler_date_process - init_date_process
        interval_calc_seq = int((between_date_process.total_seconds()) / 86400)

        print(f"init_date_process: {init_date_process}")
        print(f"scheduler_date_process: {scheduler_date_process}")
        print(f"between_date_process: {between_date_process}")
        print(f"interval_calc_seq: {interval_calc_seq}")

        etl_sql_to_gsheet_data(interval_process_seq=interval_calc_seq)

    etl_df_sql_data = PythonOperator(
        task_id='etl_df_sql_data_id',
        python_callable=etl_process_check_date
    )
