import unittest
import io
import pandas as pd
import pandas.util.testing as pdut
from parsers import csv_parser


class TestCsvParser(unittest.TestCase):

    def test_parse_raw_file_1(self):
        config_data = {
            "fileSpecs": {
                "version":"1.0",
                "delimiter": "{",
                "knownType": [
                    {
                        "rowType": "r1",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": [
                            {"fieldName": "f1", "dataType": "string"},
                            {"fieldName": "f2", "dataType": "string"},
                            {"fieldName": "f3", "dataType": "string"},
                            {"fieldName": "f4", "dataType": "string"},
                            {"fieldName": "f5", "dataType": "string"},
                            {"fieldName": "f6", "dataType": "string"}
                        ]
                    },
                    {
                        "rowType": "r2",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": [
                            {"fieldName": "f1", "dataType": "string"},
                            {"fieldName": "f2", "dataType": "string"}
                        ]
                    }
                ]
            }
        }

        raw_data = io.BytesIO("W-2{192355{266940{303570{7653.0{SEE FRAC FOCUS".encode())
        raw_data1 = io.BytesIO("W-2{192355{266940{303570{7653.0{SEE FRAC FOCUS".encode())

        df1 = pd.read_csv(raw_data, delimiter=config_data["fileSpecs"]["delimiter"], header=None,
                          names=['Field0', 'Field1', 'Field2', 'Field3', 'Field4', 'Field5'], dtype='str')

        pdut.assert_frame_equal(csv_parser.parse_dat_file(raw_data1, config_data), df1)

    def test_parse_raw_file_2(self):
        config_data = {
            "fileSpecs": {
                "version":"1.0",
                "knownType": [
                    {
                        "rowType": "r1",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": [
                            {"fieldName": "f1", "dataType": "string"},
                            {"fieldName": "f2", "dataType": "string"},
                            {"fieldName": "f3", "dataType": "string"},
                            {"fieldName": "f4", "dataType": "string"},
                            {"fieldName": "f5", "dataType": "string"},
                            {"fieldName": "f6", "dataType": "string"}
                        ]
                    },
                    {
                        "rowType": "r2",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": [
                            {"fieldName": "f1", "dataType": "string"},
                            {"fieldName": "f2", "dataType": "string"}
                        ]
                    }
                ]
            }
        }

        raw_data = io.BytesIO("W-2{192355{266940{303570{7653.0{SEE FRAC FOCUS".encode())
        self.assertRaises(ValueError, csv_parser.parse_dat_file, raw_data, config_data)

    def test_parse_raw_file_3(self):
        config_data = {
            "fileSpecs": {
                "version":"1.0",
                "delimiter": "|",
                "knownType": [
                    {
                        "rowType": "r1",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": [
                            {"fieldName": "f1", "dataType": "string"},
                            {"fieldName": "f2", "dataType": "string"},
                            {"fieldName": "f3", "dataType": "string"},
                            {"fieldName": "f4", "dataType": "string"},
                            {"fieldName": "f5", "dataType": "string"},
                            {"fieldName": "f6", "dataType": "string"}
                        ]
                    },
                    {
                        "rowType": "r2",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": [
                            {"fieldName": "f1", "dataType": "string"},
                            {"fieldName": "f2", "dataType": "string"}
                        ]
                    }
                ]
            }
        }

        raw_data = io.BytesIO("W-2{192355{266940{303570{7653.0{SEE FRAC FOCUS".encode())
        raw_data1 = io.BytesIO("W-2{192355{266940{303570{7653.0{SEE FRAC FOCUS".encode())

        df1 = pd.read_csv(raw_data, delimiter=config_data["fileSpecs"]["delimiter"], header=None,
                          names=['Field0', 'Field1', 'Field2', 'Field3', 'Field4', 'Field5'], dtype='str')
        pdut.assert_frame_equal(csv_parser.parse_dat_file(raw_data1, config_data), df1)

        raw_data = io.BytesIO("W-2|192355|266940|303570|7653.0|SEE FRAC FOCUS".encode())
        raw_data1 = io.BytesIO("W-2|192355|266940|303570|7653.0|SEE FRAC FOCUS".encode())

        df1 = pd.read_csv(raw_data, delimiter=config_data["fileSpecs"]["delimiter"], header=None,
                          names=['Field0', 'Field1', 'Field2', 'Field3', 'Field4', 'Field5'], dtype='str')
        pdut.assert_frame_equal(csv_parser.parse_dat_file(raw_data1, config_data), df1)

    def test_parse_raw_file_4(self):
        config_data = {
            "fileSpecs": {
                "version":"1.0",
                "delimiter": "{",
                "knownType": [
                    {
                        "rowType": "r1",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": [
                            {"fieldName": "f1", "dataType": "string"},
                            {"fieldName": "f2", "dataType": "string"},
                            {"fieldName": "f3", "dataType": "string"},
                            {"fieldName": "f4", "dataType": "string"},
                            {"fieldName": "f5", "dataType": "string"}
                        ]
                    },
                    {
                        "rowType": "r2",
                        "enableRow": True,
                        "fileName": "a",
                        "schema": [
                            {"fieldName": "f1", "dataType": "string"},
                            {"fieldName": "f2", "dataType": "string"}
                        ]
                    }
                ]
            }
        }

        raw_data = io.BytesIO("W-2{192355{266940{303570{7653.0{SEE FRAC FOCUS".encode())
        raw_data1 = io.BytesIO("W-2{192355{266940{303570{7653.0{SEE FRAC FOCUS".encode())

        df1 = pd.read_csv(raw_data, delimiter=config_data["fileSpecs"]["delimiter"], header=None,
                          names=['Field0', 'Field1', 'Field2', 'Field3', 'Field4'], dtype='str')

        pdut.assert_frame_equal(csv_parser.parse_dat_file(raw_data1, config_data), df1)

    def test_get_row_data_1(self):
        schema = ['f1', 'f2', 'f3']
        data = {'Field0': ['r1', 'r2', 'r3'], 'Field1': ['1', '2', '3'], 'Field2': ['a1', 'b1', 'c1']}
        df = pd.DataFrame(data)

        df1 = df[df['Field0'] == 'r1']
        df1.columns = schema
        pdut.assert_frame_equal(csv_parser.get_row_data(df, schema, 'r1'), df1)

        df1 = df[df['Field0'] == 'r2']
        df1.columns = schema
        pdut.assert_frame_equal(csv_parser.get_row_data(df, schema, 'r2'), df1)

        df1 = df[df['Field0'] == 'r3']
        df1.columns = schema
        pdut.assert_frame_equal(csv_parser.get_row_data(df, schema, 'r3'), df1)

        df1 = df[df['Field0'] == 'r4']
        df1.columns = schema
        pdut.assert_frame_equal(csv_parser.get_row_data(df, schema, 'r4'), df1)

    def test_get_row_data_2(self):
        schema = ['f1', 'f2', 'f3', 'f4']

        data = {'Field0': ['r1', 'r2', 'r3'], 'Field1': ['1', '2', '3'], 'Field2': ['a1', 'b1', 'c1']}
        df = pd.DataFrame(data)
        self.assertRaises(ValueError, csv_parser.get_row_data, df, schema, 'r1')

        data = {'Field0': ['r1', 'r2', 'r3'], 'Field1': ['1', '2', '3']}
        df = pd.DataFrame(data)
        self.assertRaises(ValueError, csv_parser.get_row_data, df, schema, 'r1')

        data = {'Field0': ['r1', 'r2', 'r3']}
        df = pd.DataFrame(data)
        self.assertRaises(ValueError, csv_parser.get_row_data, df, schema, 'r1')

        data = {}
        df = pd.DataFrame(data)
        self.assertRaises(ValueError, csv_parser.get_row_data, df, schema, 'r1')

    def test_get_row_data_3(self):
        schema = ['f1', 'f2']
        data = {'Field0': ['r1', 'r2', 'r3'], 'Field1': ['1', '2', '3'], 'Field2': ['a1', 'b1', 'c1']}
        df = pd.DataFrame(data)
        df1 = df[df['Field0'] == 'r1']
        df2 = df1.drop(df.columns[len(schema):], axis=1)
        df2.columns = schema
        pdut.assert_frame_equal(csv_parser.get_row_data(df, schema, 'r1'), df2)

        schema = ['f1']
        data = {'Field0': ['r1', 'r2', 'r3'], 'Field1': ['1', '2', '3']}
        df = pd.DataFrame(data)
        df1 = df[df['Field0'] == 'r2']
        df2 = df1.drop(df.columns[len(schema):], axis=1)
        df2.columns = schema
        pdut.assert_frame_equal(csv_parser.get_row_data(df, schema, 'r2'), df2)

        schema = []
        data = {'Field0': ['r1', 'r2', 'r3']}
        df = pd.DataFrame(data)
        df1 = df[df['Field0'] == 'r3']
        df2 = df1.drop(df.columns[len(schema):], axis=1)
        df2.columns = schema
        pdut.assert_frame_equal(csv_parser.get_row_data(df, schema, 'r3'), df2)

    def test_get_row_data_4(self):
        schema = ['f1', 'f2']
        data = {'Field1': ['1', '2', '3'], 'Field2': ['a1', 'b1', 'c1']}
        df = pd.DataFrame(data)
        self.assertRaises(ValueError, csv_parser.get_row_data, df, schema, 'r1')

        schema = ['f1']
        data = {'Field1': ['1', '2', '3']}
        df = pd.DataFrame(data)
        self.assertRaises(ValueError, csv_parser.get_row_data, df, schema, 'r1')

        schema = []
        data = {}
        df = pd.DataFrame(data)
        self.assertRaises(ValueError, csv_parser.get_row_data, df, schema, 'r1')


    def test_parse_csv_file(self):
        file_object = io.BytesIO("one,two,three,four\n1,2,3,4".encode())
        ddf = csv_parser.parse_csv_file(file_object)
        assert len(ddf) == 1
        assert len(ddf.columns) == 4
        file_object = io.BytesIO("one,two,three,four,five\n1,2,3,4,5\n11,22,33,44,55".encode())
        ddf = csv_parser.parse_csv_file(file_object)
        assert len(ddf) == 2
        assert len(ddf.columns) == 5

if __name__ == '__main__':
    unittest.main()
