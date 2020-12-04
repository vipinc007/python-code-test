import unittest
import warnings
from loaderutilities.loadertransaction import LoaderTransaction
from mock import patch


class TestLoadTransaction(unittest.TestCase):

    @classmethod
    @patch("loaderutilities.loadertransaction.LoaderTransaction.__init__", return_value=None)
    @patch("loaderutilities.loadertransaction.LoaderTransaction._id", return_value=1)
    def test_get_transaction_id(cls, mocked_property, mocked_constructor):
        warnings.simplefilter('ignore', ResourceWarning)
        objloadtransaction = LoaderTransaction("UnitTesting", 1, 'TX')
        objloadtransaction._id = mocked_property.return_value
        trans_id = objloadtransaction.get_transaction_id()
        assert 1 == trans_id

if __name__ == '__main__':
    unittest.main()
