# setup database connection to application

import mysql.connector
from db_config import config


class DatabaseConnection:
    def __init__(self):
        # initialize configuration variables
        self.host = config["host"]
        self.user = config["user"]
        self.password = config["password"]
        self.database = config["database"]
        self.connection = None

    def connect(self):
        try:
            # setup connection
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("-" * 300)
            print("\033[1m" + "Welcome to ChatDB 98" + "\033[0m")
            print("\033[1m" + "Learn how to query databases like a pro!" + "\033[0m")
            print()
        # output error statement if not able to connect to database
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            self.connection = None

    def disconnect(self):
        if self.connection:
            self.connection.close()  # disconnect message
            print("Thank you for using ChatDB!")
            print("I hope you learned something new today :)")
            print("-" * 300)

    def get_cursor(self):
        if self.connection:
            return self.connection.cursor()
        else:
            raise ConnectionError("Database connection unsuccessful :(")  # case of being unable to connect to server
