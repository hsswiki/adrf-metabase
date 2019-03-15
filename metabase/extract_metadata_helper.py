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
    elif is_code(data_cursor, col, schema_name, table_name,
                 categorical_threshold):
        return 'code'
    else:
        # is_code creates a column in the temporary table
        # metadata.converted_data with type text and the a copy of col. is_copy
        # must be run before assigning type as text.
        return 'text'


def is_numeric(data_cursor, col, schema_name, table_name):
    """Return True if column is numeric.

    Return True if column is numeric. Converts text column to numeric and
    stores it in temporary table metabase.converted_data.

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
    it in temporary table metabase.converted_data.

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
    except (psycopg2.ProgrammingError, psycopg2.DataError):
        data_cursor.execute("DROP TABLE IF EXISTS converted_data")
        return False


def is_code(data_cursor, col, schema_name, table_name,
            categorical_threshold):
    """Return True if column is categorical.

    Return True if column categorical. Stores a copy of the column in
    metabase.converted_data. Note: Even if the column is not categorical, the
    column is copied to metadata.converted_metadata as a text column and the
    column will be assumed to be text.

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

    update_column_info(metabase_cursor, col, data_table_id, 'numeric')
    # Update created by, created date.

    (minimum, maximum, mean, median) = get_numeric_metadata(data_cursor, col,
                                                            data_table_id)

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

    (max_len, min_len, median_len) = get_text_metadata(data_cursor, col,
                                                       data_table_id)

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

    update_column_info(metabase_cursor, col, data_table_id, 'date')

    (minimum, maximum) = get_date_metadata(data_cursor, col, data_table_id)

    metabase_cursor.execute(
        """
        INSERT INTO metabase.date_column
        (
        data_table_id,
        column_name,
        min_date,
        max_date,
        updated_by,
        date_last_updated
        )
        VALUES
        (
        %(data_table_id)s,
        %(column_name)s,
        %(min_date)s,
        %(max_date)s,
        %(updated_by)s,
        (SELECT CURRENT_TIMESTAMP)
        )
        """,
        {
            'data_table_id': data_table_id,
            'column_name': col,
            'min_date': minimum,
            'max_date': maximum,
            'updated_by': getpass.getuser(),
        }
        )


def get_date_metadata(data_cursor, col, data_table_id):
    """Get metadata from a date column."""

    data_cursor.execute(
        """
        SELECT
        min(data_col),
        max(data_col)
        FROM converted_data
        """
    )

    return data_cursor.fetchall()[0]


def update_code(data_cursor, metabase_cursor, col, data_table_id):
    """Update Column Info and Code Frequency for a categorical column."""

    update_column_info(metabase_cursor, col, data_table_id, 'code')

    code_freq_tp_ls = get_code_metadata(data_cursor, col, data_table_id)

    metabase_cursor.execute(
        'CREATE TEMPORARY TABLE code_freq_temp (code TEXT, freq INT);')

    for code, freq in code_freq_tp_ls:
        metabase_cursor.execute(
            'INSERT INTO code_freq_temp (code, freq) VALUES (%s, %s);',
            [code, freq],
        )

    metabase_cursor.execute(
        """
        INSERT INTO metabase.code_frequency (
            data_table_id,
            column_name,
            code,
            frequency,
            updated_by,
            date_last_updated
        ) SELECT
            %(data_table_id)s,
            %(column_name)s,
            code,
            freq,
            %(updated_by)s,
            CURRENT_TIMESTAMP
        FROM code_freq_temp;
        """,
        {
            'data_table_id': data_table_id,
            'column_name': col,
            'updated_by': getpass.getuser(),
        },
    )

    metabase_cursor.execute('DROP TABLE code_freq_temp;')


def get_code_metadata(data_cursor, col, data_table_id):
    data_cursor.execute(
        """
        SELECT data_col AS code, COUNT(*) AS frequency
        FROM converted_data
        GROUP BY data_col
        ORDER BY data_col;
        """
    )
    code_frequency_tp_ls = data_cursor.fetchall()

    return code_frequency_tp_ls


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
