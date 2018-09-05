"""
Purpose: Methods associated with USGS Earthquake Data Requests
Author: Conrad Hilley (conradhilley@gmail.com)

API Documentation: https://earthquake.usgs.gov/fdsnws/event/1/
"""

import requests

# Constants
USGS_FORMATS = ['.geojson', '.csv', '.quakeml']
USGS_TIME_PERIODS = ['month', 'week', 'day', 'hour']
USGS_MAGNITUDES = ['all', '1.0', '2.5', '4.5', 'significant']


class USGSSummary:
    # TODO Non-summary style requests
    def __init__(self, fmt='.geojson', period='month', magnitude='all'):
        self.fmt = fmt
        self.period = period
        self.magnitude = magnitude
        self.validate()

    def validate(self):
        if self.fmt not in USGS_FORMATS:
            raise ValueError('Invalid format requested ({})'.format(
                    ', '.join(USGS_FORMATS)))
        if self.period not in USGS_TIME_PERIODS:
            raise ValueError('Invalid time period requested ({})'.format(
                    ', '.join(USGS_TIME_PERIODS)))
        if self.magnitude not in USGS_MAGNITUDES:
            raise ValueError('Invalid magnitude requested ({})'.format(
                    ', '.join(USGS_MAGNITUDES)))

    def request_data(self, out_file=None):
        """Request data from usgs.gov, optionally write as text to out_file"""
        base_url = r'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/'
        _url = '{base_url}{magnitude}_{time_period}{out_fmt}'.format(
                base_url=base_url,
                magnitude=self.magnitude,
                time_period=self.period,
                out_fmt=self.fmt)

        # Perform request
        r = requests.get(_url)

        # Write as text if out_file is specified
        if out_file:
            with open(out_file, 'w') as wf:
                wf.write(r.text)

        return r
