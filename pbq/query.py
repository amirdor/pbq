from google.cloud import bigquery
import json
import requests
from functools import lru_cache

TERA_IN_BYTES = 1024 * 1024 * 1024 * 1024.0
PRICE_PER_TERA = 5.0


class Query(object):
    """this class control the query, validate, read sql files and return the price.

    Attributes
    ------
    query : str
        the query
    client : Client
        the client object for bigquery


    Methods
    ------
    price()
        return the query price

    validate()
        validate that the query can run without errors


    Static Methods
    ------
    read_file(query_file, parameters=None)
        parse query from file, the query can be with parameters inside the file


    Examples
    ------
    generate Query object with simple query

    >>> from pbq import Query
    >>> query = Query("select * from table")

    get query price

    >>> from pbq import Query
    >>> query = Query("select * from table")
    >>> print("the query price:", query.price)
    # the query price: 0.312

    validate query

    >>> from pbq import Query
    >>> query = Query("select * from table")
    >>> if not query.validate():
    >>>     raise RuntimeError("table not valid")

    query with parameters

    >>> from pbq import Query
    >>> query = Query("select * from table where user_id={user_id}", parameters={'user_id': 123})
    >>> print(query.query)
    # select * from table where user_id=123

    read query from file with parameters

    >>> from pbq import Query
    >>> query = Query.read_file('file_path.sql', parameters={'user_id':123})
    >>> print(query.query)
    # select * from table where user_id=123

    """

    def __init__(self, query, parameters=None, project=None):
        """
        :param query: str
            the query

        :param parameters: dict
            key value parameters to change values dynamically inside the query

        :param project: str
            the BQ project
        """
        self.query = query.replace('"', '\'')
        self._parameters = parameters if parameters else dict()
        if project:
            self.client = bigquery.Client(project=project)
        else:
            self.client = bigquery.Client()
        self._format()

    def _format(self):
        from string import Formatter

        # logger.info("building your stunning query")
        names = [fn for _, fn, _, _ in Formatter().parse(self.query) if fn is not None]

        if len(names) == 0:
            return

        self._validate_params(names)

        self.query = self.query.format_map(self._parameters)
        # logger.info("finish building your query")

    def _validate_params(self, names):
        missing_keys = set(names) - set(self._parameters.keys())
        if len(missing_keys) != 0:
            # logger.error(
            #     "not all the params in the query are set in the config file. missing params: {}".format(missing_keys))
            raise ValueError("not all the params in the query are set in the config file. \n missing params: {}".format(
                missing_keys))

    @staticmethod
    def _js_r(filename):
        with open(filename) as f_in:
            return json.load(f_in)

    @property
    @lru_cache(1)
    def price(self):
        """check the cost of the query

        :return: float
            the price of the query
        """
        res = self._init_query_command()
        if res is None:
            raise RuntimeError("Query is not valid, please fix your query first")
        price = int(res.total_bytes_billed) / TERA_IN_BYTES * PRICE_PER_TERA
        return round(price, 4)

    @lru_cache(1)
    def validate(self):
        """validate the query

        :return: Boolean
            True if the query is runnable

        :raise: RuntimeError
            on query error
        """
        res = self._init_query_command()
        if res is None:
            raise RuntimeError("Query is not valid, please fix your query first")
        return True

    @staticmethod
    def read_file(query_file, parameters=None, project=None):
        """parse query from file

        :param query_file: str
            path to the query file

        :param parameters: dict
            key value parameters to change values dynamically inside the query

        :return: Query object
        """
        q_file = open(query_file, 'r')
        _query = q_file.read()
        return Query(_query, parameters, project=project)

    @lru_cache(1)
    def _init_query_command(self):
        _query = self.query
        job_config = bigquery.QueryJobConfig()
        job_config.dry_run = True
        job_config.use_query_cache = False
        try:
            query_job = self.client.query(query=_query, job_config=job_config, )  # API request
        except requests.exceptions.HTTPError:
            query_job = None
        return query_job
