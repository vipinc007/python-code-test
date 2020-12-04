import unittest
from parsers import xml_parser
from mock import patch
from xml.dom import minidom


class TestXmlParser(unittest.TestCase):
    xmlstring = "<?xml version='1.0'?><Students><Student><Name>Vipin</Name><Gender>Male</Gender><Mark><English>80</English><Hindi>93</Hindi><Maths>77</Maths></Mark></Student><Student><Name>Sudha</Name><Gender>Female</Gender></Student></Students>"

    def test_replace_invalid_characters(self):
        result = xml_parser.XMLParser.replace_invalid_characters("<test>&#x0;</test>")
        self.assertEqual("<test></test>", result)
        result = xml_parser.XMLParser.replace_invalid_characters("<test>&#x0;&#x0;</test>")
        self.assertEqual("<test></test>", result)
        result = xml_parser.XMLParser.replace_invalid_characters("<test>\uFFFF</test>")
        self.assertEqual("<test></test>", result)

    def test_get_node_text(self):
        xmlstring = "<?xml version='1.0'?><Students><Student><Name>Vipin</Name><Gender>Male</Gender></Student><Student><Name>Sudha</Name><Gender>Female</Gender></Student></Students>"
        xdoc = minidom.parseString(xmlstring)
        namenodes = xdoc.getElementsByTagName("Name")
        result = xml_parser.XMLParser.get_node_text(namenodes[0])
        self.assertEqual("Vipin", result)
        result = xml_parser.XMLParser.get_node_text(namenodes[1])
        self.assertEqual("Sudha", result)
        gendernodes = xdoc.getElementsByTagName("Gender")
        result = xml_parser.XMLParser.get_node_text(gendernodes[0])
        self.assertEqual("Male", result)
        result = xml_parser.XMLParser.get_node_text(gendernodes[1])
        self.assertEqual("Female", result)

    @patch("parsers.xml_parser.XMLParser.__init__", return_value=None)
    @patch("parsers.xml_parser.XMLParser._doc", return_value=minidom.parseString(xmlstring))
    def test_parse(self, mocked_property, mocked_constructor):
        config_data = {
            "fileSpecs": {
                "knownType": [
                    {
                        "rowType": "Student",
                        "enableRow": True,
                        "fileName": "Student",
                        "schema": [
                            {"fieldName": "Name", "dataType": "string"},
                            {"fieldName": "Gender", "dataType": "string"}
                        ]
                    },
                    {
                        "rowType": "Mark",
                        "enableRow": True,
                        "fileName": "Mark",
                        "schema": [
                            {"fieldName": "English", "dataType": "integer"},
                            {"fieldName": "Hindi", "dataType": "integer"},
                            {"fieldName": "Maths", "dataType": "integer"}
                        ]
                    },
                    {
                        "rowType": "Address",
                        "enableRow": True,
                        "fileName": "Address",
                        "schema": [
                            {"fieldName": "City", "dataType": "string"},
                            {"fieldName": "Pincode", "dataType": "string"}
                        ]
                    }
                ]
            }
        }
        objxml_parser = xml_parser.XMLParser('some.xml')
        objxml_parser._doc = mocked_property.return_value
        ddf = objxml_parser.parse(config_data['fileSpecs']['knownType'][0])
        self.assertEqual(2, len(ddf))
        self.assertEqual(2, len(ddf.columns))
        ddf = objxml_parser.parse(config_data['fileSpecs']['knownType'][1])
        self.assertEqual(1, len(ddf))
        self.assertEqual(3, len(ddf.columns))
        ddf = objxml_parser.parse(config_data['fileSpecs']['knownType'][2])
        self.assertEqual(None, ddf)

    @patch("parsers.xml_parser.XMLParser.__init__", return_value=None)
    @patch("parsers.xml_parser.XMLParser._doc", return_value=minidom.parseString(xmlstring))
    def test_check_tag_exists(self, mocked_property, mocked_constructor):
        objxml_parser = xml_parser.XMLParser('some.xml')
        objxml_parser._doc = mocked_property.return_value
        result = objxml_parser.check_tag_exists("teachers")
        self.assertEqual(False, result)
        result = objxml_parser.check_tag_exists("Students")
        self.assertEqual(True, result)

    @patch("parsers.xml_parser.XMLParser.__init__", return_value=None)
    @patch("parsers.xml_parser.XMLParser._doc", return_value=minidom.parseString(xmlstring))
    def test_get_element_value_by_tagname(self, mocked_property, mocked_constructor):
        objxml_parser = xml_parser.XMLParser('some.xml')
        objxml_parser._doc = mocked_property.return_value
        result = objxml_parser.get_element_value_by_tagname("Gender")
        self.assertEqual("Male", result)
        result = objxml_parser.get_element_value_by_tagname("teachers")
        self.assertEqual(None, result)

if __name__ == '__main__':
    unittest.main()
