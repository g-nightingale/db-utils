import sqlite3
import pandas as pd


class DatabaseController:
    """
    A class to control a sqlite database
    """

    def __init__(self, dbfile):
        self.dbfile = dbfile

    def create_table(self, create_string):
        """
        Create a table
        """

        self._access_database(create_string)

    def drop_table(self, table_name):
        """
        Drop a table
        """

        self._access_database(f"DROP TABLE IF EXISTS {table_name}")

    def add_data(self, table, records):
        """
        Add data to a table
        """

        # Populate the tables with data
        for record in records:
            values = ','.join([f"'{x}'" if isinstance(x, str) else str(x) for x in record])
            self._access_database(f"INSERT INTO {table} VALUES ({values})")

    def get_table(self, table):
        """
        Get all data from a table
        """

        return self._access_database_with_result(f"SELECT * FROM {table};", table)

    def _access_database(self, query, parameters=()):
        """
        Execute a database query
        """
        connect = sqlite3.connect(self.dbfile)
        cursor = connect.cursor()
        cursor.execute(query, parameters)
        connect.commit()
        connect.close()

    def _access_database_with_result(self, query, table_name=None, pandas_df=True, parameters=()):
        """
        Execute a database query and return results in a Pandas DataFrame
        """
        connect = sqlite3.connect(self.dbfile)
        cursor = connect.cursor()
        rows = cursor.execute(query, parameters).fetchall()

        if pandas_df:
            # Create a DataFrame
            df = pd.DataFrame(rows)

            # Get column names
            if table_name is not None:
                cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
                col_names = [tup[1] for tup in cursor.fetchall()]
                df.columns = col_names

            connect.commit()
            connect.close()

            return df
        else:
            connect.commit()
            connect.close()

            return rows
