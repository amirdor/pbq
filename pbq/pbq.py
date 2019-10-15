# -*- coding: utf-8 -*-

"""Main module."""
from google.cloud import bigquery
from pbq.query import Query
from google.cloud import bigquery_storage_v1beta1
import pandas as pd
import datetime


class PBQ(object):

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
        :param save_query: boolean - if to save the query to a table also
        :param params: when `save_query` flag is on you need to give the relevant params
        :return: pd.DataFrame with the query results
        """
        job_config = bigquery.QueryJobConfig()
        if save_query:
            table_ref = self.client.dataset(params['dataset']).table(params['table'])
            job_config.destination = table_ref

        query_job = self.client.query(query=self.query, job_config=job_config)
        result = query_job.results()
        df = result.to_dataframe(bqstorage_client=self.bqstorage_client)
        return df

    def to_csv(self, filename, sep=',', save_query=False, **params):
        """
        save the query results to a csv file

        in order to save the query to a table as well as getting the dataframe, send a dict as params with:
        - table
        - dataset
        it will save to the same project
        :param filename: str - with the path to save the file
        :param sep: str separator to the csv file
        :param save_query: boolean - if to save the query to a table also
        :param params: when `save_query` flag is on you need to give the relevant params
        """
        df = self.to_dataframe(save_query, **params)
        df.to_csv(filename, sep=sep)

    def save_to_table(self, table, dataset, project=None):
        """
        save query to table
        :param table: str - table name
        :param dataset: str - data set name
        :param project: str - project name
        """
        job_config = bigquery.QueryJobConfig()
        # Set the destination table
        client = self.client
        table_ref = client.dataset(dataset).table(table)

        if project:
            table_ref = client.dataset(dataset, project=project).table(table)

        job_config.destination = table_ref
        query_job = client.query(self.query, job_config=job_config)
        query_job.results()
        print('Query results loaded to table {}'.format(table_ref.path))

    @staticmethod
    def save_file_to_table(filename, table, dataset, project, file_format=bigquery.SourceFormat.CSV, max_bad_records=0,
                           replace=True, partition=None):
        """

        :param filename: str - with the path to save the file
        :param table: str - table name
        :param dataset: str - data set name
        :param project: str - project name
        :param file_format: str - possible file format (CSV, PARQUET) (default: CSV)
        :param max_bad_records: number of bad records allowed in file (default: 0)
        :param replace: if set as true -  it will replace the table, else append to table (default: True)
        :param partition: str - partition format DDMMYYY (default: None)
        :return:
        """
        client = bigquery.Client(project=project)

        dataset_ref = client.dataset(dataset)
        if partition:
            table = '{0}${1}'.format(table, partition)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.LoadJobConfig()
        job_config.max_bad_records = max_bad_records
        job_config.source_format = file_format
        if replace:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        if file_format == bigquery.SourceFormat.CSV:
            job_config.skip_leading_rows = 1
        job_config.autodetect = True

        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()  # Waits for table load to complete.
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset, table))

    @staticmethod
    def save_dataframe_to_table(df: pd.DataFrame, table, dataset, project, max_bad_records=0, replace=True,
                                partition=None):
        """
        save pd.DataFrame object to table
        :param df: pd.DataFrame - the dataframe you want to save
        :param table: str - table name
        :param dataset: str - data set name
        :param project: str - project name
        :param max_bad_records: number of bad records allowed in file (default: 0)
        :param replace: if set as true -  it will replace the table, else append to table (default: True)
        :param partition: str - partition format DDMMYYY (default: None)
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
        {'last_modified_time': table.modified, 'num_bytes': table.num_bytes, 'num_rows': table.num_rows,
               'creation_time': table.created}
        :param table: str - table name
        :param dataset: str - data set name
        :param project: str - project name
        :return: dict - with some table information like, last_modified_time, num_bytes, num_rows, and creation_time
        """
        dataset_ref = self.client.dataset(dataset, project=project)
        table_ref = dataset_ref.table(table)
        table = self.client.get_table(table_ref)

        res = {'last_modified_time': table.modified, 'num_bytes': table.num_bytes, 'num_rows': table.num_rows,
               'creation_time': table.created}
        return res
