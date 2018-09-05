"""
Purpose: Config file functions
Author: Conrad Hilley (conradhilley@gmail.com)
"""

from configparser import ConfigParser


def config(config_file='database.ini', section='postgresql'):
    # Create a parser
    parser = ConfigParser()
    # Read config file
    parser.read(config_file)

    # Get section
    param_dict = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            param_dict[param[0]] = param[1]
    else:
        raise Exception(
                'Section {0} not found in {1}'.format(section, config_file))

    return param_dict
