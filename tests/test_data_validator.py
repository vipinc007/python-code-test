import unittest
import warnings
from loaderutilities import data_validator


class Testdata_validator(unittest.TestCase):

    def test_validate_length(self):
        valid = data_validator.validate_length("Name", "goodvalue", 9)
        assert valid is None
        valid = data_validator.validate_length("Name", "badvalue", 9)
        assert valid is not None
        valid = data_validator.validate_length("Name", "awrongvalue", 9)
        assert valid is not None
        valid = data_validator.validate_length("Name", "", 9)
        assert valid is not None

    def test_validate_min_max_length(self):
        valid = data_validator.validate_min_max_length("Name", "goodvalue", 5, 9)
        assert valid is None
        valid = data_validator.validate_min_max_length("Name", "okvalue", 5, 9)
        assert valid is None
        valid = data_validator.validate_min_max_length("Name", "awrongvalue", 5, 9)
        assert valid is not None
        valid = data_validator.validate_min_max_length("Name", "bad", 5, 9)
        assert valid is not None
        valid = data_validator.validate_min_max_length("Name", "", 5, 9)
        assert valid is not None

    def test_validate_numeric(self):
        valid = data_validator.validate_numeric("Name", "1234")
        assert valid is None
        valid = data_validator.validate_numeric("Name", "999999999999999")
        assert valid is None
        valid = data_validator.validate_numeric("Name", "123a")
        assert valid is not None
        valid = data_validator.validate_numeric("Name", "")
        assert valid is not None
        valid = data_validator.validate_numeric("Name", "123.05")
        assert valid is not None

    def test_validate_number(self):
        valid = data_validator.validate_number("Name", "1234")
        assert valid is None
        valid = data_validator.validate_number("Name", "999999999999999")
        assert valid is None
        valid = data_validator.validate_number("Name", "123a")
        assert valid is not None
        valid = data_validator.validate_number("Name", "")
        assert valid is not None
        valid = data_validator.validate_number("Name", "123.05")
        assert valid is None

    def test_validate_boolean(self):
        valid = data_validator.validate_boolean("isValid", "true")
        assert valid is None
        valid = data_validator.validate_boolean("isValid", "false")
        assert valid is None
        valid = data_validator.validate_boolean("isValid", "123a")
        assert valid is not None
        valid = data_validator.validate_boolean("isValid", "")
        assert valid is not None
        valid = data_validator.validate_boolean("isValid", "123.05")
        assert valid is not None

    def test_validate_date(self):
        valid = data_validator.validate_date("Name", "01/01/2018", "%m/%d/%Y")
        assert valid is None
        valid = data_validator.validate_date("Name", "10/25/2018", "%m/%d/%Y")
        assert valid is None
        valid = data_validator.validate_date("Name", "02/31/2018", "%m/%d/%Y")
        assert valid is not None
        valid = data_validator.validate_date("Name", "a1/b1/2018", "%m/%d/%Y")
        assert valid is not None
        valid = data_validator.validate_date("Name", "01012018", "%m/%d/%Y")
        assert valid is not None
        valid = data_validator.validate_date("Name", "01/31/2018", "%d/%m/%Y")
        assert valid is not None
        valid = data_validator.validate_date("Name", "01/31/2018", "%Y/%m/%d")
        assert valid is not None


if __name__ == '__main__':
    unittest.main()
