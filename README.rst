###############
ADRF Metabase
###############

Tools for handling metadata associated with administrative data sets.

The current version of this repo contains scripts for creating the Metabase tabels.

--------------
Requirements
--------------

- Postgres version 9.5

- Python 3.5

- See requirements.txt (``pip install -r requirements.txt``)

-----------------------
Prepare the database
-----------------------

Create super user `metaadmin` and store credentials in .pgpass file.

Create schema `metabase`

------------------------
Run migration script
------------------------

Currently there is only one version of the database. You can create all the tables by running:

``alembic upgrade head``
