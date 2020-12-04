import unittest
from manager.tx_data_ingestion_manager import TXDataIngestionManager


class TestTXDataIngestionManager(unittest.TestCase):

    def setUp(self):
        config_data = '{"fileSpecs": {"version":"1.0", "delimiter": "a", ' \
                      '"knownType": [{"rowType": "r1", "enableRow": "true", ' \
                      '"fileName": "a", "schema": [{"fieldName": "f1", "dataType": "string"}, ' \
                      '{"fieldName": "f2", "dataType": "string"}, ' \
                      '{"fieldName": "f3", "dataType": "string"}]}]}}'

        self.objParser = TXDataIngestionManager(str(config_data))

        config_data_1 = '{"fileSpecs": {"version":"1.0", "delimiter": "a", ' \
                        '"knownType": [{"rowType": "r1", "enableRow": "true", ' \
                        '"fileName": "a", "schema": [{"fieldName": "f1", "dataType": "string"}]}]}}'

        self.objParser_1 = TXDataIngestionManager(str(config_data_1))

        config_data_2 = '{"fileSpecs": {"version":"1.0", "delimiter": "a", ' \
                        '"knownType": [{"rowType": "r1", "enableRow": "true", ' \
                        '"fileName": "a", "schema": [{"fieldName": "f1", "dataType": "string"}, ' \
                        '{"fieldName": "f2", "dataType": "string"}]}]}}'

        self.objParser_2 = TXDataIngestionManager(str(config_data_2))

        config_data_3 = '{"fileSpecs": {"version":"1.0", "delimiter": "a", ' \
                        '"knownType": [{"rowType": "r1", ' \
                        '"enableRow": "true", "fileName": "a", "schema": []}]}}'

        self.objParser_3 = TXDataIngestionManager(str(config_data_3))

        config_data_4 = '{"fileSpecs": {"version":"1.0", "delimiter": "a", ' \
                        '"knownType": [{"rowType": "r1", "fileName": "a", "schema": []}, ' \
                        '{"rowType": "r2", "enableRow": "true", "fileName": "a", "enableRow": "true", ' \
                        '"schema": [{"fieldName": "f1", "dataType": "string"}]}]}}'

        self.objParser_4 = TXDataIngestionManager(str(config_data_4))


if __name__ == '__main__':
    unittest.main()
