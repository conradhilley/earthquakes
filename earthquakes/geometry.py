"""
Purpose: Geometry based functions
Author: Conrad Hilley (conradhilley@gmail.com)
"""

#TODO choose standard geometry library, shapely, ogr

def return_antipode(latitude, longitude):
    """Return antipode latitude and longitude

    Args:
        latitude (float)
        longitude (float)
    """
    if longitude < 0:
        return -1. * latitude, longitude + 180
    elif longitude >= 0:
        return -1. * latitude, longitude - 180