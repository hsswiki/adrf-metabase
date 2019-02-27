'''Tests for extract_metadata.py'''

import unittest
from unittest.mock import MagicMock, patch

import testing.postgresql

from metabase import extract_metadata


class ExtractMetaDataTest(unittest.TestCase):
    '''Test for extract_metadata'''

    def setUp(self):
        '''Create fixtures.'''

        # Create temporary database for testing.
        self.postgresql = testing.postgresql.Postgresql()
        connection_params = self.postgresql.dsn()

        # Create connection string from params.
        conn_str = 'postgresql://{user}@{host}:{port}/{database}'.format(
            user=connection_params['user'],
            host=connection_params['host'],
            port=connection_params['port'],
            database=connection_params['database'],
            )
        self.connection_string = conn_str

        # Mock settings to connec to testing database. Use this database for
        # both the metabase and the data database.
        mock_params = MagicMock()
        mock_params.metabase_connection_string = conn_str
        mock_params.data_connection_string = conn_str

        with patch('metabase.extract_metadata.settings', mock_params):
            extract = extract_metadata.ExtractMetaData(data_table_id=1)
            self.extract = extract

    def tearDown(self):
        """Delete temporary database."""

        self.postgresql.stop()

    def test_process_empty_table(self):
        '''Right now, a call to process_table just raises an error.

        As the library develops, we should overwrite this test.

        '''
        with self.assertRaises(ValueError):
            self.extract.process_table()
