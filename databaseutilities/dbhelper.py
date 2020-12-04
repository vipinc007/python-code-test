"""
    Provides the facility to connect to Oracle database.
"""
import os
import pandas_oracle.tools as oradf
import numpy as np
import cx_Oracle
from logger import logger


class DBHelper:
    _conn = None
    dbtypesmapping = {
        'oracle': {'DATE': 'DATE', 'DATETIME': 'DATE', 'INT': 'NUMBER','BOOLEAN': 'VARCHAR2', 'FLOAT': 'NUMBER', 'VARCHAR': 'VARCHAR2',
                   'STRING': 'VARCHAR2', 'INTEGER': 'INTEGER', 'NUMBER': 'NUMBER', 'TIMESTAMP WITH TIME ZONE':'TIMESTAMP WITH TIME ZONE'}}
    dbDateFormats = {"%m/%d/%Y": "MM/DD/YYYY", "%Y-%m-%dT%H:%M:%S": "YYYY-MM-DD HH24:MI:SS",
                     "%Y-%m-%dT%H:%M:%Stzh:tzm":'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM',
                     "%Y-%m-%d": "YYYY-MM-DD"}

    def __init__(self):
        self._conn = None

    def _connect(self):
        """A private method to create/open a connection to the database with the configuration provided
        """
        """dsn_str = cx_Oracle.makedsn(os.environ['ORACLE_DBHOST'],
                                    os.environ['ORACLE_PORT'],
                                    service_name=os.environ['ORACLE_INSTANCE_NAME'])

        self._conn = cx_Oracle.connect(user=os.environ['ORACLE_USERNAME'],
                                        password=os.environ['ORACLE_PASSWORD'],
                                        dsn=dsn_str)"""
        self._conn = cx_Oracle.connect("connection string")

    def _disconnect(self):
        """A private method to close an open connection to the database
        """
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def insert_and_get_inserted_id(self, sql, autoincrementcolname):
        """This function will accept an insert statement and
        help us to get the inserted autoincrement value of the inserted row
        Args:
            sql: The insert statement
            autoincrementcolname : Name of the sequence / autoincrement column of a table
        Returns:
            int : the inserted new sequence value / autoincrement value
        """
        inserted_primary_key_id = None
        self._connect()
        cur = self._conn.cursor()
        try:
            statement = sql + ' returning {0} into :NEWGENIDKEY'.format(autoincrementcolname)
            new_id = cur.var(cx_Oracle.NUMBER)
            row = {'NEWGENIDKEY': new_id}
            cur.execute(statement, row)
            inserted_primary_key_id = new_id.getvalue()
            self._conn.commit()
        except Exception as ex:
            logger.error(ex)
            raise Exception("'DBHelper.insert_and_get_inserted_id', exception occured : {0}".format(ex))
        finally:
            cur.close()
            self._disconnect()

        return inserted_primary_key_id

    def fetch_dataframe(self, sql):
        """Fetches the result of the sql select statement executed on the opened connection
        Args:
            sql: The sql select query to be executed
        Returns:
            DataFrame : The results of sql command in the form of DataFrame.
        """
        results = None
        self._connect()
        try:
            results = oradf.query_to_df(sql, self._conn, 10000)
        except Exception as ex:
            logger.error(ex)
            raise Exception("'DBHelper.fetch_dataframe', exception occured : {0}".format(ex))
        finally:
            self._disconnect()

        return results

    def execute_multiple_statements(self, sql):
        """Executes multiple sql statements seperated by ; on the database
        Args:
                sql: The sql query seperated by ;  to be executed
        """
        sql = "begin {0} end;".format(sql)
        self.execute(sql)

    def execute(self, sql):
        """Executes a single sql statement on the database
        Args:
            sql: The sql query to be executed
        """
        self._connect()
        cursor = self._conn.cursor()
        self._conn.begin()
        try:
            cursor.execute(sql)
        except Exception as ex:
            self._conn.rollback()
            logger.error(ex)
            logger.error(sql)
            raise Exception("'DBHelper.execute', exception occured : {0}".format(ex))
        finally:
            self._conn.commit()
            cursor.close()
            self._disconnect()

    def object_exists(self, object_name):
        """Checks to see if a given database object exists or not
        Args:
            object_name: the object name
        Returns:
            boolean : returns true if the object exists else false
        """
        sql = DBHelper._get_object_exists_sql_query(object_name)
        df = self.fetch_dataframe(sql)
        exists = True if len(df) > 0 else False
        return exists

    def drop_object(self, object_name):
        """Drops a given database object
        Args:
            object_name: the object name
        """
        sql = DBHelper._get_object_exists_sql_query(object_name)
        df = self.fetch_dataframe(sql)
        exists = True if len(df) > 0 else False
        if exists:
            drop_statement = "DROP {0} {1}".format(df.at[0, 'OBJECT_TYPE'], object_name)
            self.execute(drop_statement)

    def insert_dataframe_to_table(self, frame, tablename, if_exists='fail'):
        """Executes the DML sql statement on the opened connection
            Args:
                frame: The data frame that has to be inserted to oracle
                tablename: The name of the table to which the dataframe will be inserted.
                if_exists: can takes values like 'fail,new,append,replace'
            Returns:
                boolean : True if no error , False if failed
        """
        self._create_table_using_frame_schema(frame, tablename, if_exists)
        results = True
        self._connect()
        try:
            if len(frame) > 0:
                oradf.insert_multiple(tablename, frame, self._conn, 100000)
        except Exception as ex:
            results = False
            logger.error('Failed inserting records from dataframe to table : ' + tablename)
            logger.error(ex)
            raise Exception("'DBHelper.insert_dataframe_to_table', exception occured : {0}".format(ex))
        finally:
            self._disconnect()
        return results

    def bulk_insert_dataframe_to_table(self, frames, if_exists='fail'):
        """Executes the DML sql statement on the opened connection
            Args:
                frames: The dictionary of data frames that has to be inserted to oracle
                if_exists: can take values like 'fail,new,append,replace'
        """
        if if_exists is not None and if_exists != "tableexists":
            logger.debug("Check if table exists to import")
            for key, val in frames.items():
                tablename = key
                frame = val
                self._create_table_using_frame_schema(frame, tablename, if_exists)

        self._connect()
        for key, val in frames.items():
            tablename = key
            frame = val
            try:
                if len(frame) > 0:
                    oradf.insert_multiple(tablename, frame, self._conn, 100000)

            except Exception as ex:
                logger.error('Failed inserting records from dataframe to table : ' + tablename)
                logger.error(ex)
                raise Exception("'DBHelper.bulk_insert_dataframe_to_table', exception occured : {0}".format(ex))

        self._disconnect()
        logger.debug("Ending Bulk Insert of Dataframes")

    @staticmethod
    def _get_create_table_script_from_frame(frame, tablename):
        """This function returns a create table script using the columns from the frame
        Args:
            frame: the dataframe with columns
            tablename: the name of the table used in the create script
        Returns:
            string : the create statement which can be executed in a database
        """
        column_types = []
        dtypes = frame.dtypes
        for i, k in enumerate(dtypes.index):
            dt = dtypes[k]
            sqltype = DBHelper.get_db_datatype(dt.type)
            colname = DBHelper.db_colname(k)
            column_types.append((colname, sqltype))
        columns = ', '.join('%s %s' % x for x in column_types)
        template_create = """CREATE TABLE %(name)s (%(columns)s);"""
        create = template_create % {'name': tablename, 'columns': columns}
        return create

    @staticmethod
    def db_colname(pandas_colname):
        """convert pandas column name to a DBMS column name
             deal with name length restrictions, esp for Oracle
        Args:
            pandas_colname: the column name from pandas dataframe
        Returns:
            string : returns formated column name that suits to oracle
        """
        colname = pandas_colname.replace(' ', '_').strip()
        colname = colname.replace('-', '')
        colname = '"' + colname + '"'
        return colname

    @staticmethod
    def get_db_datatype(df_datatype):
        """This method gets the appropriate oracle datatype for a given datatype
        Args:
            df_datatype: the column name that needs to be verified or converted
        Returns:
            string : returns column name that suits to oracle
        """
        flavor = 'oracle'
        types = DBHelper.dbtypesmapping[flavor]  # deal with datatype differences
        if str(df_datatype) == "<type 'numpy.datetime64'>":
            sqltype = types['DATETIME']
        elif issubclass(df_datatype, np.datetime64):
            sqltype = types['DATETIME']
        elif issubclass(df_datatype, (np.integer, np.bool_)):
            sqltype = types['INT']
        elif issubclass(df_datatype, np.floating):
            sqltype = types['FLOAT']
        else:
            sqltype = types['VARCHAR'] + '(?)'.replace('?', '4000')

        return sqltype

    @staticmethod
    def _get_object_exists_sql_query(object_name):
        sql = "select OBJECT_NAME, OBJECT_TYPE from USER_OBJECTS where OBJECT_NAME='{0}'".format(object_name.upper())
        return sql
