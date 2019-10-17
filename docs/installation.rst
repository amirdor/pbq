.. highlight:: shell

============
Installation
============


Stable release
--------------

To install PBQ, run this command in your terminal:

.. code-block:: console

    $ pip install pbq

This is the preferred method to install PBQ, as it will always install the most recent stable release.

If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/


From sources
------------

The sources for PBQ can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/amirdor/pbq

Or download the `tarball`_:

.. code-block:: console

    $ curl -OJL https://github.com/amirdor/pbq/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install


.. _Github repo: https://github.com/amirdor/pbq
.. _tarball: https://github.com/amirdor/pbq/tarball/master


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
