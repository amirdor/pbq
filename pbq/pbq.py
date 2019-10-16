# -*- coding: utf-8 -*-

"""Main module."""
from google.cloud import bigquery
from pbq.query import Query
from google.cloud import bigquery_storage_v1beta1
from google.cloud.exceptions import NotFound
import pandas as pd
import datetime


class PBQ(object):
    """
    bigquery driver using the google official API

    Attributes
    ------
    query : str
        the query
    query_obj : Query
        pbq.Query object
    client : Client
        the client object for bigquery
    bqstorage_client : BigQueryStorageClient
        the google storage client object

    Methods
    ------
    to_dataframe(save_query=False, **params)
        return the query results as data frame

    to_csv(filename, sep=',', save_query=False, **params)
        save the query results to a csv file

    save_to_table(table, dataset, project=None, replace=True, partition=None)
        save query to table

    table_details(table, dataset, project)
        get the information about the table


    Static Methods
    ------
    save_file_to_table(filename, table, dataset, project, file_format=bigquery.SourceFormat.CSV, max_bad_records=0,
                           replace=True, partition=None)
        save file to table, it can be partitioned and it can append to existing table.
        the supported formats are CSV or PARQUET

    save_dataframe_to_table(df: pd.DataFrame, table, dataset, project, max_bad_records=0, replace=True,
                                partition=None)
        same as save file just with pandas dataframe

    table_exists(client: bigquery.Client, table_ref: bigquery.table.TableReference)
        check if table exists - if True - table exists else not exists


    Examples
    ------
    getting query to dataframe

    >>> from pbq import Query, PBQ
    >>> query = Query("select * from table")

    >>> print("the query price:", query.price)

    >>> if not query.validate():
    >>>     raise RuntimeError("table not valid")

    >>> pbq = PBQ(query)
    >>> pbq.to_dataframe()

    saving query to csv

    >>> from pbq import Query, PBQ
    >>> query = Query("select * from table")
    >>> pbq = PBQ(query)
    >>> pbq.to_csv()

    saving dataframe to table

    >>> import pandas as pd
    >>> from pbq import Query, PBQ
    >>> df = pd.DataFrame()

    >>> PBQ.save_dataframe_to_table(df, 'table', 'dataset', 'project_id', partition='20191013', replace=False)
    """

    def __init__(self, query: Query):
        """
        bigquery driver using the google official API
        :param query: Query object


        """
        self.query = query.query
        self.query_obj = query
        self.client = bigquery.Client()
        self.bqstorage_client = bigquery_storage_v1beta1.BigQueryStorageClient()

    def to_dataframe(self, save_query=False, **params):
        """
        return the query results as data frame

        in order to save the query to a table as well as getting the dataframe, send a dict as params with:
        - table
        - dataset
        it will save to the same project

        :param save_query: boolean
            if to save the query to a table also

        :param params: dict
            when `save_query` flag is on you need to give the relevant params

        :return: pd.DataFrame
         the query results
        """
        job_config = bigquery.QueryJobConfig()
        if save_query:
            table_ref = self.client.dataset(params['dataset']).table(params['table'])
            job_config.destination = table_ref

        query_job = self.client.query(query=self.query, job_config=job_config)
        query_job_res = query_job.result()
        df = query_job_res.to_dataframe(bqstorage_client=self.bqstorage_client)
        return df

    def to_csv(self, filename, sep=',', save_query=False, **params):
        """
        save the query results to a csv file

        in order to save the query to a table as well as getting the dataframe, send a dict as params with:
        - table
        - dataset
        it will save to the same project

        :param filename: str
            with the path to save the file

        :param sep: str
            separator to the csv file

        :param save_query: boolean
            if to save the query to a table also

        :param params: dict
            when `save_query` flag is on you need to give the relevant params
        """
        df = self.to_dataframe(save_query, **params)
        df.to_csv(filename, sep=sep, index=False)

    def save_to_table(self, table, dataset, project=None, replace=True, partition=None):
        """
        save query to table

        :param table: str
            table name

        :param dataset: str
            data set name

        :param project: str
            project name

        :param replace: boolean
            if set as true -  it will replace the table, else append to table (default: True)

        :param partition: str
            partition format DDMMYYY (default: None)
        """
        job_config = bigquery.QueryJobConfig()
        # Set the destination table
        client = self.client
        if partition:
            table = '{0}${1}'.format(table, partition)
        table_ref = client.dataset(dataset).table(table.split('$')[0])

        exists_ok = PBQ._writing_disposition(job_config, replace)

        if project:
            table_ref = client.dataset(dataset, project=project).table(table)

        PBQ._create_table(client, exists_ok, partition, replace, table_ref)

        job_config.destination = table_ref
        query_job = client.query(self.query, job_config=job_config)
        query_job.result()
        print('Query results loaded to table {}'.format(table_ref.path))

    @staticmethod
    def _writing_disposition(job_config: bigquery.QueryJobConfig, replace):
        exists_ok = False
        if replace:
            exists_ok = True
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        return exists_ok

    @staticmethod
    def _create_table(client: bigquery.Client, exists_ok, partition, replace, table_ref):
        if (partition and not PBQ.table_exists(client, table_ref)) or (not partition and replace):
            bq_table = bigquery.Table(table_ref)
            if partition:
                time_partitioning = bigquery.TimePartitioning()
                bq_table.time_partitioning = time_partitioning
            client.create_table(bq_table, exists_ok=exists_ok)

    @staticmethod
    def save_file_to_table(filename, table, dataset, project, file_format=bigquery.SourceFormat.CSV, max_bad_records=0,
                           replace=True, partition=None):
        """
        save file to table, it can be partitioned and it can append to existing table.
        the supported formats are CSV or PARQUET

        :param filename: str
            with the path to save the file

        :param table: str
            table name

        :param dataset: str
            data set name

        :param project: str
            project name

        :param file_format: str
            possible file format (CSV, PARQUET) (default: CSV)

        :param max_bad_records: int
            number of bad records allowed in file (default: 0)

        :param replace: boolean
            if set as trueit will replace the table, else append to table (default: True)

        :param partition: str
            partition format DDMMYYY (default: None)
        """
        client = bigquery.Client(project=project)

        dataset_ref = client.dataset(dataset)
        if partition:
            table = '{0}${1}'.format(table, partition)
        table_ref = dataset_ref.table(table.split('$')[0])
        job_config = bigquery.LoadJobConfig()
        job_config.max_bad_records = max_bad_records
        job_config.source_format = file_format
        exists_ok = PBQ._writing_disposition(job_config, replace)

        if file_format == bigquery.SourceFormat.CSV:
            job_config.skip_leading_rows = 1
        job_config.autodetect = True

        PBQ._create_table(client, exists_ok, partition, replace, table_ref)

        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()  # Waits for table load to complete.
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset, table))

    @staticmethod
    def save_dataframe_to_table(df: pd.DataFrame, table, dataset, project, max_bad_records=0, replace=True,
                                partition=None):
        """
        save pd.DataFrame object to table

        :param df: pd.DataFrame
            the dataframe you want to save

        :param table: str
            table name

        :param dataset: str
            data set name

        :param project: str
            project name

        :param max_bad_records: int
            number of bad records allowed in file (default: 0)

        :param replace: boolean
            if set as true -  it will replace the table, else append to table (default: True)

        :param partition: str
            partition format DDMMYYY (default: None)
        """
        now = datetime.datetime.now()
        random_string = '{}'.format(now.strftime('%y%m%d%H%M%S'))
        input_path = "/tmp/tmp-{}.parquet".format(random_string)
        PBQ._save_df_to_parquet(df, input_path)
        PBQ.save_file_to_table(input_path, table, dataset, project, file_format=bigquery.SourceFormat.PARQUET,
                               max_bad_records=max_bad_records, replace=replace, partition=partition)

    @staticmethod
    def _save_df_to_parquet(df, input_path, index=False):
        df.columns = ["{}".format(col) for col in df.columns]
        df.to_parquet(input_path, index=index)

    def table_details(self, table, dataset, project):
        """
        return a dict object with some details about the table

        :param table: str
            table name

        :param dataset: str
            data set name

        :param project: str
            project name

        :return: dict
            with some table information like, last_modified_time, num_bytes, num_rows, and creation_time
        """
        dataset_ref = self.client.dataset(dataset, project=project)
        table_ref = dataset_ref.table(table)
        table = self.client.get_table(table_ref)

        res = {'last_modified_time': table.modified, 'num_bytes': table.num_bytes, 'num_rows': table.num_rows,
               'creation_time': table.created}
        return res

    @staticmethod
    def table_exists(client: bigquery.Client, table_ref: bigquery.table.TableReference):
        """
        check if table exists - if True - table exists else not exists

        :param client: bigquery.Client object

        :param table_ref: bigquery.table.TableReference object
            with the table name and dataset

        :return: boolean
            True if table exists
            False if table not exists
        """
        try:
            table = client.get_table(table_ref)
            if table:
                return True
        except NotFound as error:
            return False
