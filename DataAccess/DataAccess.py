import pymysql
import os
from sqlalchemy import create_engine

class DataAccess():

    def __init__(self, Username = None, Password = None, Host = None, Port = None, Database = None):
        self.Username = Username 
        self.Password = Password 
        self.Host = Host 
        self.Port = Port
        self.Database = Database 

        #import pdb
        #pdb.set_trace()

        self.engine = create_engine(
            'mysql+pymysql://' +
            self.Username + ':' + self.Password + '@' + self.Host + ':' + self.Port + '/' + self.Database#,
            #pool_size=10, 
            #max_overflow=20
        )

        self.connection = self.engine.connect()
        self.raw_connection = self.engine.raw_connection()
        self.cursor = self.engine.raw_connection().cursor()
        self.transaction = None
    
    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        self.raw_connection.close()
        self.engine.dispose()

    def begin_transaction(self):
        self.transaction = self.connection.begin()

    def commit_transaction(self):
        self.transaction.commit()

    def rollback_transaction(self):
        self.transaction.rollback()