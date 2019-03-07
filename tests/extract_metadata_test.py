'''Tests for extract_metadata.py'''

import unittest
from unittest.mock import MagicMock, patch

import alembic.config
from alembic.config import Config
import sqlalchemy

from sqlalchemy.ext.automap import automap_base

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
        cls.engine = engine

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

        engine.execute('create table data.numeric_1 '
                       '(c1 int primary key, c2 numeric)')
        engine.execute('insert into data.numeric_1 values (1, 1.1)')
        engine.execute('insert into data.numeric_1 values (2, 2.2)')
        # TODO create text, date, and categorical testing tables.

        # Update metabase.data_table for tables created above.
        engine.execute("""insert into metabase.data_table
                          (data_table_id, file_table_name)
                          values (1, 'data.numeric_1')""")
        # Generate mapped classes from database.
        Base = automap_base(
            bind=engine,
            metadata=sqlalchemy.MetaData(schema='metabase')
            )
        Base.prepare(engine, reflect=True)
        cls.data_table = Base.classes.data_table

    @classmethod
    def tearDownClass(self):
        """Delete temporary database."""

        self.postgresql.stop()

    def test_process_empty_table(self):
        '''Right now, a call to process_table just raises an error.

        As the library develops, we should overwrite this test.

        '''
        with self.assertRaises(ValueError):
            self.extract.process_table()

    def test_row_count(self):
        '''Test that the row count is correct'''

        # TODO remove this line once the actual extract_metadata method is
        # working.
        self.engine.execute('''update metabase.data_table
                               set number_rows = 123
                               where data_table_id = 1''')

        data_table = self.data_table
        row_count = sqlalchemy.sql.expression.select(
            columns=[sqlalchemy.text('number_rows')],
            from_obj=data_table,
        ).where(data_table.data_table_id == 1)
        conn = self.engine.connect()
        result = conn.execute(row_count).fetchall()
        self.assertEqual(123, result[0][0])
