'''Tests for extract_metadata.py'''

import unittest
from unittest.mock import MagicMock, patch

import alembic.config
from alembic.config import Config
import sqlalchemy
import testing.postgresql

from metabase import extract_metadata


class ExtractMetaDataTest(unittest.TestCase):
    '''Test for extract_metadata'''

    @classmethod
    def setUpClass(cls):
        '''Create database fixtures.'''

        # Create temporary database for testing.
        cls.postgresql = testing.postgresql.Postgresql()
        connection_params = cls.postgresql.dsn()

        # Create connection string from params.
        conn_str = 'postgresql://{user}@{host}:{port}/{database}'.format(
            user=connection_params['user'],
            host=connection_params['host'],
            port=connection_params['port'],
            database=connection_params['database'],
            )
        cls.connection_string = conn_str

        # Create metabase schema.
        engine = sqlalchemy.create_engine(conn_str)
        engine.execute(sqlalchemy.schema.CreateSchema('metabase'))

        # Run alembic scripts to create database tables.
        alembic_cfg = Config()
        alembic_cfg.set_main_option('script_location', 'alembic')
        alembic_cfg.set_main_option('sqlalchemy.url', conn_str)
        alembic.command.upgrade(alembic_cfg, 'head')

        # Mock settings to connec to testing database. Use this database for
        # both the metabase and the data database.
        mock_params = MagicMock()
        mock_params.metabase_connection_string = conn_str
        mock_params.data_connection_string = conn_str

        with patch('metabase.extract_metadata.settings', mock_params):
            extract = extract_metadata.ExtractMetaData(data_table_id=1)
            cls.extract = extract

        # Create data schema and tables.
        engine.execute(sqlalchemy.schema.CreateSchema('data'))

    def tearDown(self):
        """Delete temporary database."""

        self.postgresql.stop()

    def test_process_empty_table(self):
        '''Right now, a call to process_table just raises an error.

        As the library develops, we should overwrite this test.

        '''
        with self.assertRaises(ValueError):
            self.extract.process_table()
