"""Helper funtions for extract_metadata.
"""

import getpass

import psycopg2
from psycopg2 import sql


def get_column_type(data_cursor, col, categorical_threshold, schema_name,
                    table_name):
    """Return the column type."""

    if is_numeric(data_cursor, col, schema_name, table_name):
        return 'numeric'
    elif is_date(data_cursor, col, schema_name, table_name):
        return 'date'
    elif is_code(data_cursor, col, schema_name, table_name, categorical_threshold):
        return 'code'
    else:
        # is_code creates a column in metadata.temp with type text and the a
        # copy of col. is_copy must be run before assigning type as text.
        return 'text'

def is_numeric(data_cursor, col, schema_name, table_name):
    """Return True if column is numeric.

    Return True if column is numeric. Converts text column to numeric and
    stores it in metabase.temp.

    """

    # Create temporary table for storing converted data.
    data_cursor.execute(
        """
        CREATE TEMPORARY TABLE IF NOT EXISTS
        converted_data
        (data_col NUMERIC)
        """
    )

    try:
        data_cursor.execute(
            sql.SQL("""
            INSERT INTO converted_data (data_col)
            SELECT {}::NUMERIC FROM {}.{}
            """).format(
                sql.Identifier(col),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        return True
    except (psycopg2.ProgrammingError, psycopg2.DataError):
        data_cursor.execute('DROP TABLE IF EXISTS converted_data')
        return False


def is_date(data_cursor, col, schema_name, table_name):
    """Return True if column is date.

    Return True if column is type date. Converts text column to date and stores
    it in metabase.temp.

    """

    # Create temporary table for storing converted data.
    data_cursor.execute(
        """
        CREATE TEMPORARY TABLE IF NOT EXISTS
        converted_data
        (data_col DATE);
        TRUNCATE TABLE converted_data;
        """
    )

    try:
        data_cursor.execute(
            sql.SQL("""
            INSERT INTO converted_data (data_col)
            SELECT {}::DATE FROM {}.{}
            """).format(
                sql.Identifier(col),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        return True
    except (psycopg2.ProgrammingError, psycopg2.DataError) as e:
        data_cursor.execute("DROP TABLE IF EXISTS converted_data")
        return False


def is_code(data_cursor, col, schema_name, table_name,
            categorical_threshold):
    """Return True if column is categorical.

    Return True if column categorical. Stores a copy of the column in
    metabase.temp. Note: Even if the column is not categorical, the column is
    copied to metadata.temp as a text column and the column will be assumed to
    be text.

    """

    # Create temporary table for storing converted data.
    data_cursor.execute(
        """
        CREATE TEMPORARY TABLE IF NOT EXISTS
        converted_data
        (data_col TEXT);
        TRUNCATE TABLE converted_data;
        """
    )

    data_cursor.execute(
        sql.SQL(
            """
            SELECT COUNT(DISTINCT {}) FROM {}.{}
            """).format(
            sql.Identifier(col),
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
    )
    n_distinct = data_cursor.fetchall()[0][0]

    data_cursor.execute(sql.SQL("""
        INSERT INTO converted_data (data_col)
        SELECT {} FROM {}.{}
        """).format(
                sql.Identifier(col),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
        )
        )

    if n_distinct <= categorical_threshold:
        return True
    else:
        return False


def update_numeric(data_cursor, metabase_cursor, col, data_table_id):
    """Update Column Info  and Numeric Column for a numerical column."""

    # TODO this needs to be split into two functions, one that uses the data
    # cursor and one that uses metabase cursor to support data stored in a
    # second database.

    update_column_info(metabase_cursor, col, data_table_id, 'numeric')
    # Update created by, created date.

    (minimum, maximum, mean, median) = get_numeric_metadata(data_cursor, col, data_table_id)

    metabase_cursor.execute(
        """
        INSERT INTO metabase.numeric_column
        (
        data_table_id,
        column_name,
        minimum,
        maximum,
        mean,
        median,
        updated_by,
        date_last_updated
        )
        VALUES
        (
        %(data_table_id)s,
        %(column_name)s,
        %(minimum)s,
        %(maximum)s,
        %(mean)s,
        %(median)s,
        %(updated_by)s,
        (SELECT CURRENT_TIMESTAMP)
        )
        """,
        {
            'data_table_id': data_table_id,
            'column_name': col,
            'minimum': minimum,
            'maximum': maximum,
            'mean': mean,
            'median': median,
            'updated_by': getpass.getuser(),
        }
    )

def get_numeric_metadata(data_cursor, col, data_table_id):
    """Get metdata from a numeric column."""


    data_cursor.execute(
        """
        SELECT
        min(data_col),
        max(data_col),
        avg(data_col),
        PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY data_col)
        FROM converted_data
        """
    )

    return data_cursor.fetchall()[0]


def update_text(data_cursor, metabase_cursor, col, data_table_id):
    """Update Column Info  and Numeric Column for a numerical column."""

    update_column_info(metabase_cursor, col, data_table_id, 'text')
    # Update created by, created date.

    (max_len, min_len, median_len) = get_text_metadata(data_cursor, col, data_table_id)
    
    metabase_cursor.execute(
        """
        INSERT INTO metabase.text_column
        (
        data_table_id,
        column_name,
        max_length,
        min_length,
        median_length,
        updated_by,
        date_last_updated
        )
        VALUES
        (
        %(data_table_id)s,
        %(column_name)s,
        %(max_length)s,
        %(min_length)s,
        %(median_length)s,
        %(updated_by)s,
        (SELECT CURRENT_TIMESTAMP)
        )
        """,
        {
            'data_table_id': data_table_id,
            'column_name': col,
            'max_length': max_len,
            'min_length': min_len,
            'median_length': median_len,
            'updated_by': getpass.getuser(),
        }
    )

def get_text_metadata(data_cursor, col, data_table_id):
    """Get metadata from a text column."""

    # Create tempory table to hold text lengths.
    data_cursor.execute(
        """
        CREATE TEMPORARY TABLE text_length
        AS
        SELECT char_length(data_col)
        FROM converted_data
        """
    )

    data_cursor.execute("SELECT * FROM converted_data")

    data_cursor.execute(
        """
        SELECT
        MAX(text_length.char_length),
        MIN(text_length.char_length),
        PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY text_length.char_length)
        FROM text_length;
        """
    )
    
    (max_len, min_len, median_len) = data_cursor.fetchall()[0]

    data_cursor.execute("DROP TABLE text_length")

    return (max_len, min_len, median_len)

def update_date(data_cursor, metabase_cursor, col, data_table_id):
    """Update Column Info and Date Column for a date column."""

    pass

def get_data_metadata(data_cursor, col, data_table_id):
    """Get metadata from a date column."""

    pass


def update_column_info(cursor, col, data_table_id, data_type):
    """Add a row for this data column to the column info metadata table."""

    # TODO How to handled existing rows?

    # Create Column Info entry
    cursor.execute(
        """
        INSERT INTO metabase.column_info
        (data_table_id,
        column_name,
        data_type,
        updated_by,
        date_last_updated
        )
        VALUES
        (
        %(data_table_id)s,
        %(column_name)s,
        %(data_type)s,
        %(updated_by)s,
        (SELECT CURRENT_TIMESTAMP)
        )
        """,
        {
            'data_table_id': data_table_id,
            'column_name': col,
            'data_type': data_type,
            'updated_by': getpass.getuser(),
        }
    )
