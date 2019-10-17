=======
Usage
=======

Import this package to your project and you are ready to go

.. code-block:: python

    from pbq import PBQ
    from pbq import Query


|

PBQ
----
This class can have generic functions for big query manipulation and save queries to tables.


some usages:
*************

Download table to CSV
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from pbq import PBQ
   from pbq import Query

   # read sql file and format the query from file to string without parameters
   query = Query.read_file('queries/query.sql')

   # init the query builder
   pbq = PBQ(query)

   #run the query and get csv
   pbq.to_csv('file.csv', save_query=True, **{'table': 'table', 'dataset': 'dataset'})


when setting `save_query=True` it means that you want to save your query to a table, and you need to send dictionary with the table name and dataset name.

|
Download table to pandas DataFrame
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from pbq import PBQ
   from pbq import Query

   # read sql file and format the query from file to string without parameters
   query = Query.read_file('queries/query.sql')

   # init the query builder
   pbq = PBQ(query)

   #run the query and get dataframe
   pbq.to_dataframe()

|

Save query to a table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from pbq import PBQ
   from pbq import Query

   # read sql file and format the query from file to string without parameters
   query = Query('select * from table')

   # init the query builder
   pbq = PBQ(query)
   pbq.save_to_table('table_name', 'dataset_name', 'project_name', replace=True, partition='20190910')

When setting `replace=True` it will override the table as long as the table is not partitioned, if partitioned it will overwrite the partition.

`partition` format is YYYYMMDD

|

upload DataFrame to a table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import pandas as pd
    from pbq import Query, PBQ
    df = pd.DataFrame()

   PBQ.save_dataframe_to_table(df, 'table', 'dataset', 'project_id', partition='20191013', replace=False)


|

Query
-------
This class will format your query, check validation and return the price of the query


some usages:
*****************

generate the query object with a simple query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from pbq import Query
    query = Query("select * from table")

get query price
^^^^^^^^^^^^^^^^^

.. code-block:: python

    from pbq import Query
    query = Query("select * from table")
    print("the query price:", query.price)
    # the query price: 0.312

validate query
^^^^^^^^^^^^^^^^^

.. code-block:: python

    from pbq import Query
    query = Query("select * from table")
    if not query.validate():
        raise RuntimeError("table not valid")

query with parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from pbq import Query
    query = Query("select * from table where user_id={user_id}", parameters={'user_id': 123})
    print(query.query)
    # select * from table where user_id=123

read the query from file with parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from pbq import Query
    query = Query.read_file('file_path.sql', parameters={'user_id':123})
    print(query.query)
    # select * from table where user_id=123

|