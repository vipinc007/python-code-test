import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from commonutilities import util
from mock import patch
import numpy as np

class TestUtil(unittest.TestCase):
    def test_replace_char(self):
        df1= pd.DataFrame({'col1': ['\'01234'], 'col2': ['\'09123\'02939']})
        df2 = pd.DataFrame({'col1': ['01234'], 'col2': ['09123\'02939']})
        resultdf = util.replace_char(df1)
        for col_name in resultdf.columns:
            collect =str(resultdf.loc[:,col_name])
            expected = str(df2.loc[:,col_name])
            assert expected == collect

    def test_remove_periods(self):
        df1= pd.DataFrame({'col1': ['01.23.4'], 'col2': ['091.23.02939'], 'col3': ['091.23.02939 Test.']})
        df2 = pd.DataFrame({'col1': ['01.23.4'], 'col2': ['091.23.02939'], 'col3': ['0912302939 Test.']})
        resultdf = util.remove_periods(df1, "col3")
        for col_name in resultdf.columns:
            collect =str(resultdf.loc[:,col_name])
            expected = str(df2.loc[:,col_name])
            assert expected == collect

    def test_get_fields(self):
        self.assertEqual(util.get_fields(3), ['Field0', 'Field1', 'Field2'])

    def test_get_max_field_count_senario1(self):
        config_data = {
            "fileSpecs":{
                "version":"1.0",
                "delimiter": "a",
                "knownType": [
                    {
                        "rowType":"r1",
                        "enableRow":True,
                        "fileName":"a",
                        "schema": [
                            {"fieldName":"f1", "dataType":"string"}
                        ]
                    }
                ]
            }
        }
        self.assertEqual(util.get_max_field_count(config_data), 1)

    def test_get_max_field_count_senario2(self):
        config_data = {
            "fileSpecs":{
                "version":"1.0",
                "delimiter": "a",
                "knownType": [
                    {
                        "rowType":"r1",
                        "enableRow":True,
                        "fileName":"a",
                        "schema": [
                            {"fieldName":"f1", "dataType":"string"},
                            {"fieldName":"f2", "dataType":"string"},
                            {"fieldName":"f3", "dataType":"string"}
                        ]
                    }
                ]
            }
        }
        self.assertEqual(util.get_max_field_count(config_data), 3)

    def test_get_max_field_count_senario3(self):
        config_data = {
            "fileSpecs":{
                "version":"1.0",
                "delimiter": "a",
                "knownType": [
                    {
                        "rowType":"r1",
                        "enableRow":True,
                        "fileName":"a",
                        "schema": [
                            {"fieldName":"f1", "dataType":"string"},
                            {"fieldName":"f2", "dataType":"string"}
                        ]
                    }
                ]
            }
        }
        self.assertEqual(util.get_max_field_count(config_data), 2)

    def test_get_max_field_count_senario4(self):
        config_data = {
            "fileSpecs":{
                "version":"1.0",
                "delimiter": "a",
                "knownType": [
                    {
                        "rowType":"r1",
                        "enableRow":True,
                        "fileName":"a",
                        "schema": []
                    }
                ]
            }
        }
        self.assertEqual(util.get_max_field_count(config_data), 0)

    def test_get_all_items(self):
        schema = [
            {"fieldName":"f1", "dataType":"string"},
            {"fieldName":"f2", "dataType":"string"},
            {"fieldName":"f3", "dataType":"string"}
        ]
        self.assertEqual(util.get_all_items(schema, "fieldName"), ['f1', 'f2', 'f3'])

    def test_get_raw_schema_senario1(self):
        config_data = {
            "fileSpecs":{
                "version":"1.0",
                "delimiter": "a",
                "knownType": [
                    {
                        "rowType":"r1",
                        "enableRow":True,
                        "fileName":"a",
                        "schema": [
                            {"fieldName":"f1", "dataType":"string"},
                            {"fieldName":"f2", "dataType":"string"},
                            {"fieldName":"f3", "dataType":"string"}
                        ]
                    }
                ]
            }
        }
        self.assertEqual(util.get_raw_schema(config_data), ['Field0', 'Field1', 'Field2'])

    def test_get_raw_schema_senario2(self):
        config_data = {
            "fileSpecs":{
                "version":"1.0",
                "delimiter": "a",
                "knownType": [
                    {
                        "rowType":"r1",
                        "enableRow":True,
                        "fileName":"a",
                        "schema": [
                            {"fieldName":"f1", "dataType":"string"}
                        ]
                    }
                ]
            }
        }
        self.assertEqual(util.get_raw_schema(config_data), ['Field0'])

    def test_get_raw_schema_senario3(self):
        config_data = {
            "fileSpecs":{
                "version":"1.0",
                "delimiter": "a",
                "knownType": [
                    {
                        "rowType":"r1",
                        "enableRow":True,
                        "fileName":"a",
                        "schema": [
                            {"fieldName":"f1", "dataType":"string"},
                            {"fieldName":"f2", "dataType":"string"}
                        ]
                    }
                ]
            }
        }
        self.assertEqual(util.get_raw_schema(config_data), ['Field0', 'Field1'])

    def test_get_raw_schema_senario4(self):
        config_data = {
            "fileSpecs": {
                "version":"1.0",
                "delimiter": "a",
                "knownType": [
                    {
                        "rowType": "r1",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": []
                    }
                ]
            }
        }
        self.assertEqual(util.get_raw_schema(config_data), [])

    def test_get_raw_schema_senario5(self):
        config_data = {
            "fileSpecs":{
                "version":"1.0",
                "delimiter": "a",
                "knownType": [
                    {
                        "rowType":"r1",
                        "enableRow":True,
                        "fileName":"a",
                        "schema": []
                    },
                    {
                        "rowType": "r2",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": [
                            {"fieldName": "f1", "dataType": "string"}
                        ]
                    }
                ]
            }
        }
        self.assertEqual(util.get_raw_schema(config_data), ['Field0'])

    @patch("loaderutilities.loadertransaction.LoaderTransaction.__init__", return_value=None)
    @patch("loaderutilities.loadertransaction.LoaderTransaction._id", return_value=1)
    def test_feed_raw_dataframe_with_technical_columns(self, mocked_property, mocked_constructor):
        from loaderutilities.loadertransaction import LoaderTransaction
        objloadtransaction = LoaderTransaction("UnitTesting", 1, 'TX')
        objloadtransaction._id = mocked_property.return_value
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})
        df = util.feed_raw_dataframe_with_technical_columns(df, objloadtransaction)
        self.assertEqual('LOADER_TRANSACTIONS_ID' in df.columns, True)
        self.assertEqual('ACTIVE' in df.columns, True)
        self.assertEqual('VALIDATION_ERRORS' in df.columns, True)

    @patch("loaderutilities.loadertransaction.LoaderTransaction.__init__", return_value=None)
    @patch("loaderutilities.loadertransaction.LoaderTransaction._id", return_value=1)
    def test_feed_native_dataframe_with_technical_columns(self, mocked_property, mocked_constructor):
        from loaderutilities.loadertransaction import LoaderTransaction
        objloadtransaction = LoaderTransaction("UnitTesting", 1, 'TX')
        objloadtransaction._id = mocked_property.return_value
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})
        df = util.feed_native_dataframe_with_technical_columns(df, objloadtransaction)
        self.assertEqual('LOADER_TRANSACTIONS_ID' in df.columns, True)

    def test_replace_nan_value_in_dataframe(self):
        df = pd.DataFrame({'col1': ['nan', 2, np.nan], 'col2': [4, None, 'naneee']})
        df = util.replace_nan_value_in_dataframe(df)
        countofnan = df.isnull().sum().sum()
        self.assertEqual(0, countofnan)

    def test_get_trackingnumber_from_filename(self):
        filename = "therawfile_12345_approved.dat"
        self.assertEqual(util.get_trackingnumber_from_filename(filename), "12345")

    def test_handle_replace_attribute(self):
        input_df = pd.DataFrame({'F1': ["tes't", "2", "hello"], 'F2': ["value2", "well", 'naneee']})
        schema = {
                "rowType": "r1",
                "enableRow": True,
                "fileName": "a",
                "schema": [
                    {"fieldName": "F1", "dataType": "string", "replace": [{"'": ""}]},
                    {"fieldName": "F2", "dataType": "string"}
                ]
            }

        expected_df = pd.DataFrame({'F1': ["test", "2", "hello"], 'F2': ["value2", "well", 'naneee']})
        actual_df = util.handle_replace_attribute(input_df, schema)
        assert_frame_equal(expected_df, actual_df, check_dtype=False)


if __name__ == '__main__':
    unittest.main()
