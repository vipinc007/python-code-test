import unittest
import warnings
from dataprocessor import ok_raw_data_processor
import pandas as pd
from mock import patch


class TestOKRawDataProcessor(unittest.TestCase):

    def test_generate_sqlscripts(self):
        dframes = {}
        dframes["mykey"] = pd.DataFrame({'API_NUMBER': [1, 2, 3], 'col2': [4, 5, 6]})
        sql_script = ok_raw_data_processor.generate_sqlscripts(dframes,"LC",10)
        assert sql_script == ' UPDATE LC SET API_NUMBER=\'1\' WHERE ID=10'

if __name__ == '__main__':
    unittest.main()
