"""
Provides convenient methods to do data validation.
"""
from datetime import datetime


def validate_length(column_name, value, length):
    """This method helps to validate length of a given value
    Args:
        column_name: Name of the column to construct the error message
        value: value to be validated
        length: length to be validated
    Returns:
        if invalid returns error message else None
    """
    valuelength = len(value)
    if valuelength > int(length) >= 0:
        return "{0} : value '{1}' is greater than the specified length {2}".format(column_name, value, length)
    elif valuelength < int(length) and int(length) >= 0:
        return "{0} : value '{1}' is less than the specified length {2}".format(column_name, value, length)

    return None


def validate_min_max_length(column_name, value, min_length, max_length):
    """This method helps to validate min and max length of a given value
    Args:
        column_name: Name of the column to construct the error message
        value: value to be validated
        min_length: min length to be validated
        max_length: max length to be validated
    Returns:
        if invalid returns error message else None
    """
    valuelength = len(value)
    if (valuelength < int(min_length) or valuelength > int(max_length) and (
            int(min_length) >= 0 and int(max_length) >= 0)):
        return "{0} : Length of value '{1}' is not in range of {2}-{3}".format(column_name, value, min_length,
                                                                               max_length)
    return None


def validate_numeric(column_name, value, column_data_type="numeric"):
    """This method helps to validate if a given value is numeric
    Args:
        column_name: Name of the column to construct the error message
        value: value to be validated
        column_data_type: datatype to construct the error message
    Returns:
        if invalid returns error message else None
    """
    valid = value.isnumeric()
    if not valid:
        return "{0} : '{1}' is not a valid {2}".format(column_name, value, column_data_type)
    return None


def validate_boolean(column_name, value, column_data_type="boolean"):
    """This method helps to validate if a given value is boolean
    Args:
        column_name: Name of the column to construct the error message
        value: value to be validated
        column_data_type: datatype to construct the error message
    Returns:
        if invalid returns error message else None
    """
    if value.lower() not in ("true", "false", "y", "n"):
        return "{0} : '{1}' is not a valid {2}".format(column_name, value, column_data_type)
    return None


def validate_number(column_name, value, column_data_type="number"):
    """This method helps to validate if a given value is a number
    Args:
        column_name: Name of the column to construct the error message
        value: value to be validated
        column_data_type: datatype to construct the error message
    Returns:
        if invalid returns error message else None
    """
    valid = value.isnumeric()
    if valid is False:
        try:
            float(value)
            return None
        except ValueError:
            return "{0} : '{1}' is not a valid {2}".format(column_name, value, column_data_type)
    return None


def validate_date(column_name, value, date_format, column_data_type="date"):
    """This method helps to validate if a given value is a valid date
    Args:
        column_name: Name of the column to construct the error message
        value: value to be validated
        date_format: existing date format of the value
        column_data_type: datatype to construct the error message
    Returns:
        if invalid returns error message else None
    """
    value = value.replace("T", " ")
    dtpart = value.split(" ")
    value = dtpart[0]
    try:
        datetime.strptime(value, date_format)
        return None
    except ValueError:
        return "{0} : '{1}' is not a valid {2}".format(column_name, value, column_data_type)


def validate_timestamp(column_name, value, date_format, column_data_type="timestamp"):
    """This method helps to validate if a given value is a valid date
    Args:
        column_name: Name of the column to construct the error message
        value: value to be validated
        date_format: existing date format of the value
        column_data_type: datatype to construct the error message
    Returns:
        if invalid returns error message else None
    """
    value = value.replace("T", " ")
    date_format = date_format.replace("T", " ")
    date_value, time_value = value.split(" ")
    format_firstpart, format_secondpart = date_format.split('tzh')
    if "-" in time_value:
        time_value, tz_value = time_value.split("-")
        value = "{0} {1}".format(date_value, time_value)

        if time_value is not None and tz_value is not None:
            try:
                datetime.strptime(value, format_firstpart)
                return None
            except ValueError:
                return "{0} : '{1}' is not a valid {2}".format(column_name, value, column_data_type)
    elif "+" in time_value:
        time_value, tz_value = time_value.split("+")
        value = "{0} {1}".format(date_value, time_value)
        if time_value is not None and tz_value is not None:
            try:
                datetime.strptime(value, format_firstpart)
                return None
            except ValueError:
                return "{0} : '{1}' is not a valid {2}".format(column_name, value, column_data_type)
