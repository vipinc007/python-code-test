import unittest
import warnings
from dataprocessor import raw_data_processor
import pandas as pd
from mock import patch


class TestRawDataProcessor(unittest.TestCase):

    def test_validate_datatype(self):
        coljson = {"fieldName": "mycol", "dataType": "integer"}
        validationmessage = raw_data_processor.validate_datatype("mycol", "abc", "integer", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "abc", "date", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "123", "integer", coljson)
        assert validationmessage is None
        validationmessage = raw_data_processor.validate_datatype("mycol", "ssdsd", "string", coljson)
        assert validationmessage is None
        validationmessage = raw_data_processor.validate_datatype("mycol", "123", "number", coljson)
        assert validationmessage is None

        coljson = {"fieldName": "mycol", "dataType": "integer", "length": "6"}
        validationmessage = raw_data_processor.validate_datatype("mycol", "123", "integer", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "1234567", "integer", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "121212", "integer", coljson)
        assert validationmessage is None

        coljson = {"fieldName": "mycol", "dataType": "integer", "minLength": "5",	"maxLength": "6"}
        validationmessage = raw_data_processor.validate_datatype("mycol", "123", "integer", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "1234567", "integer", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "12121", "integer", coljson)
        assert validationmessage is None
        validationmessage = raw_data_processor.validate_datatype("mycol", "121214", "integer", coljson)
        assert validationmessage is None

        coljson = {"fieldName": "mycol", "dataType": "date", "format": "%m/%d/%Y"}
        validationmessage = raw_data_processor.validate_datatype("mycol", "02/31/2018", "date", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "13/31/2018", "date", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "01/31/2018", "date", coljson)
        assert validationmessage is None

        coljson = {"fieldName": "mycol", "dataType": "number"}
        validationmessage = raw_data_processor.validate_datatype("mycol", "02/31/2018", "number", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "a123", "number", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", " 1 2 3 ", "number", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "#343", "number", coljson)
        assert validationmessage is not None
        validationmessage = raw_data_processor.validate_datatype("mycol", "12", "number", coljson)
        assert validationmessage is None
        validationmessage = raw_data_processor.validate_datatype("mycol", "12.34", "number", coljson)
        assert validationmessage is None

    @patch("databaseutilities.dbhelper.DBHelper.fetch_dataframe", return_value=pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]}))
    def test_check_for_schema_difference(self, mocked_method):
        warnings.simplefilter('ignore', ResourceWarning)
        existingdataframe = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})
        diffcols = raw_data_processor.check_for_schema_difference("emp", existingdataframe)
        assert len(diffcols) == 0
        existingdataframe = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6], 'col3': [2, 4, 6]})
        diffcols = raw_data_processor.check_for_schema_difference("emp", existingdataframe)
        assert len(diffcols) == 1

    def test_validate_dataframe_datatype(self):
        config_data = [
                            {"fieldName": "f1", "dataType": "string", "minLength":"2", "maxLength":"4"},
                            {"fieldName": "f2", "dataType": "string"}
                        ]
        existingdataframe = pd.DataFrame(
            {'f1': ['a', 'bc', 'cedopq'],
             'f2': ['fgh', 'ijk', 'lmn'],
             "VALIDATION_ERRORS": ['','',''],
             "ACTIVE":[True, True, True]
            }
        )
        frame = raw_data_processor.validate_dataframe_datatype(existingdataframe,config_data)
        vdf = frame[frame['ACTIVE'] == False]
        assert len(vdf) == 2

        config_data = [
            {"fieldName": "f1", "dataType": "string", "minLength": "1", "maxLength": "4"},
            {"fieldName": "f2", "dataType": "string"}
        ]
        existingdataframe = pd.DataFrame(
            {'f1': ['ad', 'bc', 'ced'],
             'f2': ['fgh', 'ijk', 'lmn'],
             "VALIDATION_ERRORS": ['', '', ''],
             "ACTIVE": [True, True, True]
             }
        )
        frame = raw_data_processor.validate_dataframe_datatype(existingdataframe, config_data)
        vdf = frame[frame['ACTIVE'] == False]
        assert len(vdf) == 0


    def test_refactor_error_dataframes(self):
        config_data1 = [
            {"fieldName": "f1", "dataType": "string", "minLength": "2", "maxLength": "4"},
            {"fieldName": "f2", "dataType": "string"}
        ]
        config_data2 = {
            "fileSpecs": {
                "knownType": [
                    {
                        "rowType": "Student",
                        "enableRow": True,
                        "fileName": "Student",
                        "schema": [
                            {"fieldName": "F1", "dataType": "string", "minLength": "2", "maxLength": "4"},
                            {"fieldName": "F2", "dataType": "string"}
                        ]
                    }
                ]
            }
        }
        existingdataframe = {}
        studentframe = pd.DataFrame(
            {'F1': ['a', 'bc', 'cedopq'],
             'F2': ['fgh', 'ijk', 'lmn'],
             "VALIDATION_ERRORS": ['','',''],
             "ACTIVE":[True, True, True]
            }
        )
        studentframe = raw_data_processor.validate_dataframe_datatype(studentframe, config_data1)
        existingdataframe['Student'] = studentframe
        frame = raw_data_processor.refactor_error_dataframes(existingdataframe, "" ,config_data2)
        frame = frame['Student']
        vdf = frame[frame['F2'] == ""]
        assert len(vdf) == 2


    def test_refactor_native_dataframes(self):
        config_data1 = [
            {"fieldName": "f1", "dataType": "string", "minLength": "2", "maxLength": "4"},
            {"fieldName": "f2", "dataType": "string"}
        ]
        config_data2 = {
            "fileSpecs": {
                "knownType": [
                    {
                        "rowType": "Student",
                        "enableRow": True,
                        "fileName": "Student",
                        "schema": [
                            {"fieldName": "F1", "dataType": "string", "minLength": "2", "maxLength": "4"},
                            {"fieldName": "F2", "dataType": "string"}
                        ]
                    }
                ]
            }
        }
        existingdataframe = {}
        studentframe = pd.DataFrame(
            {'F1': ['a', 'bc', 'cedopq'],
             'F2': ['fgh', 'ijk', 'lmn'],
             "VALIDATION_ERRORS": ['','',''],
             "ACTIVE":[True, True, True]
            }
        )
        studentframe = raw_data_processor.validate_dataframe_datatype(studentframe, config_data1)
        existingdataframe['Student'] = studentframe
        frame = raw_data_processor.refactor_native_dataframes(existingdataframe, "" ,config_data2)
        frame = frame['Student']
        vdf = frame[frame['F1'] == "bc"]
        assert len(vdf) == 1




if __name__ == '__main__':
    unittest.main()
