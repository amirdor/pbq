===
PBQ
===


.. image:: https://img.shields.io/pypi/v/pbq.svg
        :target: https://pypi.python.org/pypi/pbq

.. image:: https://img.shields.io/travis/amirdor/pbq.svg
        :target: https://travis-ci.org/amirdor/pbq

.. image:: https://readthedocs.org/projects/pbq/badge/?version=latest
        :target: https://pbq.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

python BiqQuery driver for easy access


|

Installing
----------

To start using this package run:

.. code-block:: bash

   $ pip install pbq




For development use, and local testing run:

.. code-block:: bash

    $ python setup.py install



|


Dependency
----------

**bq Command-Line Tool**

Install bq cli -- the package is running bq cli in the background

.. code-block:: bash

   https://cloud.google.com/sdk/docs/
after installation on a new terminal write:

.. code-block:: bash

   $ bq init
   $ gcloud auth application-default login

if you don't want to install bq cli you can run the package with the `service account key`

all you need to do it to define an environment variable like this:

.. code-block:: bash

    $ export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/[FILE_NAME].json"


|


.. include:: usage.rst

* Free software: MIT license
* Documentation: https://pbq.readthedocs.io.
