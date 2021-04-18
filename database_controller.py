import sqlite3


class DatabaseController:
    """
    A class to control a sqlite database
    """

    def __init__(self, dbfile):
        self.dbfile = dbfile

    def access_database(self, query, parameters=()):
        """
        Execute a database query
        """
        connect = sqlite3.connect(self.dbfile)
        cursor = connect.cursor()
        cursor.execute(query, parameters)
        connect.commit()
        connect.close()

    def access_database_with_result(self, query, parameters=()):
        """
        Execute a database query and return results
        """
        connect = sqlite3.connect(self.dbfile)
        cursor = connect.cursor()
        rows = cursor.execute(query, parameters).fetchall()
        connect.commit()
        connect.close()
        return rows

    def create_table(self, create_string):
        """
        Create a table
        """
        self.access_database(create_string)

    def drop_table(self, table_name):
        """
        Drop a table
        """

        self.access_database(f"DROP TABLE IF EXISTS {table_name}")

    def add_data(self, table, records):
        """
        Add data to a table
        """

        # Populate the tables with data
        for record in records:
            values = ','.join(map(str, record))
            self.access_database(f"INSERT INTO {table} VALUES ({values})")

    def get_table(self, table):
        """
        Get all data from a table
        """

        return self.access_database_with_result(f"SELECT * FROM {table};")
