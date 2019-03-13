"""

"""

import getpass

import psycopg2
from psycopg2 import sql


def get_column_type(cursor, col, categorical_threshold, schema_name,
                    table_name):
    """ """


    if is_numeric(cursor, col, schema_name, table_name):
        return 'numeric'
    elif is_date(cursor, col, schema_name, table_name):
        return 'date'
    elif is_code(cursor, col, schema_name, table_name, categorical_threshold):
        return 'code'
    else:
        # is_code creates a column in metadata.temp with type text and the a
        # copy of col. is_copy must be run before assigning type as text.
        return 'text'


def is_numeric(cursor, col, schema_name, table_name):
    """Return True if column is numeric.

    Return True if column is numeric. Converts text column to numeric and
    stores it in metabase.temp.

    """

    cursor.execute("DROP TABLE IF EXISTS metabase.temp")
    cursor.execute(
        "CREATE TABLE metabase.temp (converted_data NUMERIC)"
    )
    try:
        cursor.execute(
            sql.SQL("""
            INSERT INTO metabase.temp (converted_data)
            SELECT {}::NUMERIC FROM {}.{}
            """).format(
                sql.Identifier(col),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        return True
    except (psycopg2.ProgrammingError, psycopg2.DataError):
        cursor.execute("DROP TABLE IF EXISTS metabase.temp")
        return False


def is_date(cursor, col, schema_name, table_name):
    """Return True if column is date.

    Return True if column is type date. Converts text column to date and stores
    it in metabase.temp.

    """

    cursor.execute("DROP TABLE IF EXISTS metabase.temp")
    cursor.execute(
        "CREATE TABLE metabase.temp (converted_data DATE)"
    )
    try:
        cursor.execute(
            sql.SQL("""
            INSERT INTO metabase.temp (converted_data)
            SELECT {}::DATE FROM {}.{}
            """).format(
                sql.Identifier(col),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        return True
    except (psycopg2.ProgrammingError, psycopg2.DataError):
        cursor.execute("DROP TABLE IF EXISTS metabase.temp")
        return False

def is_code(cursor, col, schema_name, table_name,
            categorical_threshold):
    """Return True if column is categorical.

    Return True if column categorical. Stores a copy of the column in
    metabase.temp. Note: Even if the column is not categorical, the column is
    copied to metadata.temp as a text column and the column will be assumed to
    be text.

    """ 

    cursor.execute("DROP TABLE IF EXISTS metabase.temp")
    cursor.execute(
        "CREATE TABLE metabase.temp (converted_data TEXT)"
    )
    cursor.execute(
        sql.SQL(
            """
            SELECT COUNT(DISTINCT {}) FROM {}.{}
            """).format(
            sql.Identifier(col),
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
    )
    n_distinct = cursor.fetchall()[0][0]

    if n_distinct <= categorical_threshold:
        cursor.execute(sql.SQL("""
        INSERT INTO metabase.temp (converted_data)
        SELECT {} FROM {}.{}
        """).format(
                sql.Identifier(col),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
        )
        )
        return True
    else:
        return False

def update_numeric(cursor, col, data_table_id):
    """ """

    #TODO this needs to be split into two functions, one that uses the data
    # cursor and one that uses metabase cursor to support data stored in a
    # second database.

    #Create Column Info entry
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
            'data_type': 'numeric',
            'updated_by': getpass.getuser(),
        }
    )

    # Update created by, created date.

    # Get metatdata and update Column Info.
    cursor.execute(
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
            SELECT
            %(data_table_id)s,
            %(column_name)s,
            min(converted_data),
            max(converted_data),
            avg(converted_data),
            PERCENTILE_CONT(0.5)
                WITHIN GROUP (ORDER BY converted_data),
            %(updated_by)s,
            (SELECT CURRENT_TIMESTAMP)
            FROM metabase.temp
            """,
        {
        'data_table_id': data_table_id,
        'column_name': col,
        'updated_by': getpass.getuser(),
        }
    )
