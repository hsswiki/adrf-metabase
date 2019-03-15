"""Tests for extract_metadata.py"""

import datetime
import unittest
from unittest.mock import MagicMock, patch

import alembic.config
from alembic.config import Config
import pytest
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
import testing.postgresql

from metabase import extract_metadata


class ExtractMetadataTest(unittest.TestCase):
    """Test for extract_metadata"""

    @classmethod
    def setUpClass(cls):
        """Create database fixtures."""

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

        # Create `metabase` and `data` schemata.
        engine = sqlalchemy.create_engine(conn_str)
        engine.execute(sqlalchemy.schema.CreateSchema('metabase'))
        engine.execute(sqlalchemy.schema.CreateSchema('data'))
        cls.engine = engine

        # Create metabase tables with alembic scripts.
        alembic_cfg = Config()
        alembic_cfg.set_main_option('script_location', 'alembic')
        alembic_cfg.set_main_option('sqlalchemy.url', conn_str)
        alembic.command.upgrade(alembic_cfg, 'head')

        # TODO: Move database setup codes into each individual tests.
        engine.execute('create table data.numeric_1 '
                       '(c1 int primary key, c2 numeric)')
        engine.execute('insert into data.numeric_1 values (1, 1.1)')
        engine.execute('insert into data.numeric_1 values (2, 2.2)')

        # TODO: create text, date, and categorical testing tables.

        # Mock settings to connect to testing database. Use this database for
        # both the metabase and data schemata.
        mock_params = MagicMock()
        mock_params.metabase_connection_string = conn_str
        mock_params.data_connection_string = conn_str
        cls.mock_params = mock_params

        # Generate mapped classes from database.
        Base = automap_base(
            bind=engine,
            metadata=sqlalchemy.MetaData(schema='metabase')
            )
        Base.prepare(engine, reflect=True)
        cls.data_table = Base.classes.data_table

    @classmethod
    def tearDownClass(cls):
        """Delete temporary database."""

        cls.postgresql.stop()

    def tearDown(self):
        self.engine.execute("TRUNCATE TABLE metabase.data_table CASCADE")
        self.engine.execute("TRUNCATE TABLE metabase.numeric_column CASCADE")
        self.engine.execute("TRUNCATE TABLE metabase.numeric_column")
        self.engine.execute("TRUNCATE TABLE metabase.text_column")
        self.engine.execute("TRUNCATE TABLE metabase.date_column")
        self.engine.execute('DROP TABLE IF EXISTS data.table_1')

    def _test_row_count(self):
        """Test that the row count is correct"""

        # TODO remove this line once the actual extract_metadata method is
        # working.
        self.engine.execute("""update metabase.data_table
                               set number_rows = 123
                               where data_table_id = 1""")

        data_table = self.data_table
        row_count = sqlalchemy.sql.expression.select(
            columns=[sqlalchemy.text('number_rows')],
            from_obj=data_table,
        ).where(data_table.data_table_id == 1)
        conn = self.engine.connect()
        result = conn.execute(row_count).fetchall()
        self.assertEqual(123, result[0][0])

    def test_get_table_name_no_data_table(self):
        """
        Test the validity of `data_table_id` as an argument to the constructor
        of ExtractMetadata.
        """
        with pytest.raises(ValueError):
            # Will raise error since `metabase.data_table` is empty
            with patch('metabase.extract_metadata.settings', self.mock_params):
                extract_metadata.ExtractMetadata(data_table_id=1)

    def test_get_table_name_one_data_table(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.data_table_name');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        assert (('data', 'data_table_name')
                == (extract.schema_name, extract.table_name))

    def test_get_table_name_multiple_data_table(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table
                (data_table_id, file_table_name)
                VALUES
                    (1, 'data.data_table_name_1'),
                    (2, 'data.data_table_name_2')
            ;
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=2)

        assert (('data', 'data_table_name_2')
                == (extract.schema_name, extract.table_name))

    def test_get_table_level_metadata_num_of_rows_0_row_raise_error(self):
        """
        The following group of tests share data table `data.table_test_n_rows`:

            - test_get_table_level_metadata_num_of_rows_0_row
            - test_get_table_level_metadata_num_of_rows_1_row
            - test_get_table_level_metadata_num_of_rows_2_rows

        `data.table_test_n_rows` will be dropped at the end of the last test
        in this group.

        """
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_rows');

            CREATE TABLE data.table_test_n_rows (c1 INT PRIMARY KEY);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        with pytest.raises(ValueError):
            extract._get_table_level_metadata()

    def test_get_table_level_metadata_num_of_rows_1_row(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_rows');

            INSERT INTO data.table_test_n_rows (c1)
                VALUES (1);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        result = self.engine.execute("""
            SELECT number_rows
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_n_rows = result[0][0]

        assert 1 == result_n_rows

    def test_get_table_level_metadata_num_of_rows_2_rows(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_rows');

            INSERT INTO data.table_test_n_rows (c1)
                VALUES (2);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        self.engine.execute('DROP TABLE data.table_test_n_rows;')

        result = self.engine.execute("""
            SELECT number_rows
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_n_rows = result[0][0]

        assert 2 == result_n_rows

    def test_get_table_level_metadata_num_of_cols_0_col_raise_error(self):
        """
        The following group of tests share data table `data.table_test_n_cols`:
        TODO: update this docstring

            - test_get_table_level_metadata_num_of_cols_0_col_0_row
            - test_get_table_level_metadata_num_of_cols_1_col_0_row
            - test_get_table_level_metadata_num_of_cols_2_cols_1_row

        `data.table_test_n_cols` will be dropped at the end of the last test
        in this group.

        """
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_cols');

            CREATE TABLE data.table_test_n_cols ();
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        with pytest.raises(ValueError):
            extract._get_table_level_metadata()

    def test_get_table_level_metadata_num_of_cols_1_col_0_row_raise_error(
            self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_cols');

            ALTER TABLE data.table_test_n_cols ADD c1 INT PRIMARY KEY;
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        with pytest.raises(ValueError):
            extract._get_table_level_metadata()

    def test_get_table_level_metadata_num_of_cols_1_col_1_row(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_cols');

            INSERT INTO data.table_test_n_cols (c1) VALUES (1);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        result = self.engine.execute("""
            SELECT number_columns, number_rows
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_n_cols_n_rows = result[0]

        assert (1, 1) == result_n_cols_n_rows

    def test_get_table_level_metadata_num_of_cols_2_cols_2_row(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_cols');

            ALTER TABLE data.table_test_n_cols ADD c2 TEXT;

            INSERT INTO data.table_test_n_cols (c1, c2) VALUES (2, 'text');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        self.engine.execute('DROP TABLE data.table_test_n_cols;')

        result = self.engine.execute("""
            SELECT number_columns, number_rows
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_n_cols_n_rows = result[0]

        assert (2, 2) == result_n_cols_n_rows

    def test_get_table_level_metadata_table_size_0(self):
        """
        Share data table with

            - test_get_table_level_metadata_table_size_0
            - test_get_table_level_metadata_table_size_larger_than_0
        Will be dropped
        """
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_size');

            CREATE TABLE data.table_test_size ();
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        with pytest.raises(ValueError):
            extract._get_table_level_metadata()

    def test_get_table_level_metadata_table_size_larger_than_0(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_size');

            ALTER TABLE data.table_test_size ADD c1 INT PRIMARY KEY;

            INSERT INTO data.table_test_size (c1) VALUES (1);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        self.engine.execute('DROP TABLE data.table_test_size;')

        result = self.engine.execute("""
            SELECT size
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_table_size = result[0][0]

        assert 8192 == result_table_size

    def test_get_table_level_metadata_updated_by_user_name_not_empty(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_updated_by');

            CREATE TABLE data.table_test_updated_by (c1 INT);

            INSERT INTO data.table_test_updated_by (c1) VALUES (1);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        self.engine.execute('DROP TABLE data.table_test_updated_by;')

        result = self.engine.execute("""
            SELECT updated_by
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_updated_by_user_name = result[0][0]

        assert (isinstance(result_updated_by_user_name, str)
                and (len(result_updated_by_user_name) > 0))

    def test_get_table_level_metadata_date_last_updated_not_empty(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_date_last_updated');

            CREATE TABLE data.table_test_date_last_updated (c1 INT);

            INSERT INTO data.table_test_date_last_updated (c1) VALUES (1);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        self.engine.execute('DROP TABLE data.table_test_date_last_updated;')

        result = self.engine.execute("""
            SELECT date_last_updated
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_date_last_updated = result[0][0]

        assert isinstance(result_date_last_updated, datetime.datetime)

    def test_get_column_level_metadata__column_info(self):
        """Test extracting column level metadata into Column Info table."""

        self.engine.execute("""
           INSERT INTO metabase.data_table (data_table_id, file_table_name)
           VALUES (1, 'data.table_1');

           CREATE TABLE data.table_1
               (c_num INT, c_text TEXT, c_code TEXT, c_date DATE);

           INSERT INTO data.table_1 (c_num, c_text, c_code, c_date)
           VALUES
           (1, 'a', 'x', '2018-01-01'),
           (2, 'a', 'y', '2018-02-01'),
           (3, 'c', 'z', '2018-03-02');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_column_level_metadata(categorical_threshold=2)

        # Check column info retuls.
        results = self.engine.execute(
            "SELECT * FROM metabase.column_info"
        ).fetchall()

        assert 3 == len(results)

    def test_get_column_level_metadata_numeric(self):
        """Test extracting numeric column level metadata."""

        self.engine.execute("""
           INSERT INTO metabase.data_table (data_table_id, file_table_name)
           VALUES (1, 'data.table_1');

           CREATE TABLE data.table_1
               (c_num INT);

           INSERT INTO data.table_1 (c_num)
           VALUES
           (1),
           (2),
           (3);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_column_level_metadata(categorical_threshold=2)

        # Check Numeric results.
        results = self.engine.execute("""
            SELECT
            data_table_id,
            column_name,
            minimum,
            maximum,
            mean,
            median,
            updated_by,
            date_last_updated
            FROM metabase.numeric_column
            """
        ).fetchall()[0]

        assert results[0] == 1
        assert results[1] == 'c_num' 
        assert results[2] == 1
        assert results[3] == 3
        assert results[4] == 2
        assert results[5] == 2 
        assert isinstance(results[6], str)
        assert isinstance(results[7], datetime.datetime)

    def test_get_column_level_metadata_text(self):
        """Test extracting text column level metadata."""

        self.engine.execute("""
           INSERT INTO metabase.data_table (data_table_id, file_table_name)
           VALUES (1, 'data.table_1');

           CREATE TABLE data.table_1 (c_text TEXT);

           INSERT INTO data.table_1 (c_text)
           VALUES
           ('abc'),
           ('efgh'),
           ('ijklm');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_column_level_metadata(categorical_threshold=2)

        # Check date results.
        results = self.engine.execute("""
            SELECT
            data_table_id,
            column_name,
            max_length,
            min_length,
            median_length,
            updated_by,
            date_last_updated
            FROM metabase.text_column
            """
        ).fetchall()[0]

        assert results[0] == 1
        assert results[1] == 'c_text' 
        assert results[2] == 5
        assert results[3] == 3
        assert results[4] == 4
        assert isinstance(results[5], str)
        assert isinstance(results[6], datetime.datetime)


    def test_get_column_level_metadata_date(self):
        """Test extracting date column level metadata."""

        self.engine.execute("""
           INSERT INTO metabase.data_table (data_table_id, file_table_name)
           VALUES (1, 'data.table_1');

           CREATE TABLE data.table_1
               (c_date DATE);

           INSERT INTO data.table_1 ( c_date)
           VALUES
           ('2018-01-01'),
           ('2018-02-01'),
           ('2018-03-02');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_column_level_metadata(categorical_threshold=2)

        # Check date results.
        results = self.engine.execute("""
            SELECT
            data_table_id,
            column_name,
            min_date,
            max_date,
            updated_by,
            date_last_updated
            FROM metabase.date_column
            """
        ).fetchall()[0]

        assert results[0] == 1
        assert results[1] == 'c_date' 
        assert results[2] == datetime.date(2018, 1, 1)
        assert results[3] == datetime.date(2018, 3, 2)
        assert isinstance(results[4], str)
        assert isinstance(results[5], datetime.datetime)


