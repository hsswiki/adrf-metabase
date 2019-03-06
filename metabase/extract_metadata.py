'''Class to extract metadata from a Data Table'''

import sqlalchemy

from . import settings


class ExtractMetadata():
    '''Class to extract metadata from a Data Table.'''

    def __init__(self, data_table_id):
        '''Set Data Table ID and connect to database.

        Args:
           data_table_id (int): ID associated with this Data Table.

        '''

        self.data_table_id = data_table_id
        self.metabase_engine = sqlalchemy.create_engine(
            settings.metabase_connection_string
            )
        self.data_engine = sqlalchemy.create_engine(
            settings.data_connection_string
            )
        self.schema_name, self.table_name = self.__get_table_name()

    def process_table(self, categorical_threshold=10):
        '''Update the metabase with metadata from this Data Table.'''

        self.get_table_level_metadata()
        self.get_column_level_metadata(categorical_threshold)

    def get_table_level_metadata(self):
        '''Extract table level metadata and store it in the metabase.

        Extract table level metadata (number or rows, number of columns and
        file size (table size)) and store it in DataTable. Also set updated by
        and date last updated.

        '''

        # TODO
        pass

    def get_column_level_metadata(self, categorical_threshold):
        '''Extract column level metadata and store it in the metabase.

        Process columns one by one, identify or infer type, update Column Info
        and corresponding column table.

        '''

        column_type = self.__get_column_type(categorical_threshold)
        if column_type == 'numeric':
            self.get_numeric_metadata()
        elif column_type == 'text':
            self.get_text_metadata()
        elif column_type == 'date':
            self.get_date_metadate()
        elif column_type == 'code':
            self.get_code_metadata()
        else:
            raise ValueError('Unknow column type')

    def __get_table_name(self):
        '''Return the the table schema and name using the Data Table ID.

        Returns table name and schema name by looking up the Data Table ID in
        the metabase. The table name and schema name will be used to query the
        table itself.

        Returns:
            (str, str): (schema name, table name)

        '''
        conn = self.metabase_engine.connect()
        result = conn.execute(
            """
            SELECT file_table_name
            FROM metabase.data_table
            WHERE data_table_id = {data_table_id};
            """.format(data_table_id=self.data_table_id)
        ).fetchall()    # Return value is like `[('data.numeric_1',)]`
        
        schema_name, table_name = result[0][0].split('.')

        return schema_name, table_name

    def __get_column_type(self, categorical_threshold):
        '''Identify or infer column type.

        Uses the type set in the database if avaible. If all columns are text,
        attempts to infer the column type.

        Returns:
          str: 'numeric', 'text', 'date' or 'code'

        '''

        # TODO
        pass

    def __get_numeric_metadata(self):
        '''Extract metadata from a numeric column.

        Extract metadata from a numeric column and store metadata in Column
        Info and Numeric Column. Update relevant audit fields.

        '''

        # TODO
        pass

    def __get_text_metadata(self):
        '''Extract metadata from a text column.

        Extract metadata from a text column and store metadata in Column Info
        and Text Column. Update relevant audit fields.

        '''

        # TODO
        pass

    def __get_date_metadata(self):
        '''Extract metadata from a date column.

        Extract metadata from date column and store metadate in Column Info and
        Date Column. Update relevant audit fields.

        '''

        # TODO
        pass

    def __get_code_metadata(self):
        '''Extract metadata from a categorial column.

        Extract metadata from a categorial columns and store metadata in Column
        Info and Code Frequency. Update relevant audit fields.

        '''

        # TODO
        pass
