"""
    This class helps us to deal with the Loadertransactions creation.
    Loadertransaction will capture the filename to a table and provide an ID to the file
    which will be used by other tables for reference and to verify the filename to which the records
    belong to. In other words each file loaded is considered as a transaction and is given a transaction ID
    for reference.
"""
from databaseutilities.dbhelper import DBHelper
from logger import logger
from commonutilities import util


class LoaderTransaction:
    _objdb = DBHelper()
    _id = None
    _loader_config_id = None
    _state_code = None
    _table_name = "{0}_LOADER_TRANSACTIONS"
    _table_sequence_name = "{0}_ID"
    _filename = None

    def __init__(self, filename, loader_config_id, state_code):
        """This constructor Generates a new transaction ID
        Args:
            filename: The file name or file path which is getting parsed
            state_name: The name of the state
            state_code: The two char state code
        Raises:
            Exception('LoaderTransaction could not be created ')
        """
        self._state_code = state_code
        self._table_name = self._table_name.format(self._state_code)
        self._table_sequence_name = self._table_sequence_name.format(self._table_name)
        self._loader_config_id = loader_config_id
        self._filename = filename
        sql = None
        if self._loader_config_id == 1:  # TX
            tracking_no = "'{0}'".format(util.get_trackingnumber_from_filename(self._filename))
            sql = "INSERT INTO {0} " \
                  "(ID, LOADER_CONFIG_ID, FILE_NAME, TRACKING_NO, PACKET_ID, DATE_OF_ARRIVAL, DATE_OF_PARSING, STATUS, REMARK) " \
                  "VALUES({1}.nextval,{2},'{3}',{4},NULL, sysdate, NULL, NULL, NULL)" \
                  "".format(self._table_name, self._table_sequence_name, self._loader_config_id, filename, tracking_no)

        if self._loader_config_id == 2:  # CO
            sql = "INSERT INTO {0} " \
                  "(ID, LOADER_CONFIG_ID, FILE_NAME, DATE_OF_ARRIVAL, DATE_OF_PARSING, STATUS, REMARK, DOC_NUM, FORM_NUM, API_SEQ_NUM) " \
                  "VALUES({1}.nextval,{2},'{3}',sysdate ,NULL, NULL, NULL, NULL, NULL, NULL)" \
                  "".format(self._table_name, self._table_sequence_name, self._loader_config_id, filename)

        if self._loader_config_id == 3:  # ok
            sql = "INSERT INTO {0} " \
                  "(ID, LOADER_CONFIG_ID, FILE_NAME, DATE_OF_ARRIVAL, DATE_OF_PARSING, STATUS, REMARK) " \
                  "VALUES({1}.nextval,{2},'{3}',sysdate ,NULL, NULL, NULL)" \
                  "".format(self._table_name, self._table_sequence_name, self._loader_config_id, filename)

        self._id = self._objdb.insert_and_get_inserted_id(sql, "ID")
        if self._id is None:
            raise Exception('LoaderTransaction could not be created ')
        else:
            logger.info("Loader Transaction Created With ID: " + str(int(self._id)) + " for file: '{0}'".format(filename))

    def get_transaction_id(self):
        """Gives us the generated transaction ID
        returns :
            int : The _id value .
        """
        return self._id

    def get_loader_config_id(self):
        """Gives us the Loader config ID
        returns :
            int : The _loader_config_id value .
        """
        return self._loader_config_id

    def get_table_name(self):
        """Gives us the name of the table where loader transactions are stored
        returns : The _table_name value .
        """
        return self._table_name

    def update_loader_transaction_status(self, status, remark):
        """Save the changes to Status and Remark
        """
        sql = "UPDATE {0} SET STATUS = {1}, REMARK ='{2}', DATE_OF_PARSING = sysdate WHERE ID ={3}" \
              "".format(self._table_name, status, remark, self.get_transaction_id())
        self._objdb.execute(sql)

    def update_dateofparsing_to_currenttime(self):
        """Sets the DATE_OF_PARSING to current datetime
        """
        sql = "UPDATE {0} SET DATE_OF_PARSING = sysdate WHERE ID ={1}" \
              "".format(self._table_name, self.get_transaction_id())
        self._objdb.execute(sql)
