"""
Purpose: Postgres helper methods
Author: Conrad Hilley (conradhilley@gmail.com)
"""
from config import config
import json
import psycopg2
import usgs


class PostgresDB(object):
    def __init__(self, config_file='database.ini'):
        self.params = config(config_file=config_file, section='postgresql')
        self.conn = None

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(**self.params)
        except ConnectionError('Invalid connection parameters'):
            self.conn = None
        finally:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()


def update_usgs_data(table='earthquakes'):

    # Request data from USGS
    usgs_data = usgs.USGSSummary()
    r = usgs_data.request_data()

    # Parse earthquake json
    geojson_dict = json.loads(r.text)

    # Open connection to db and activate cursor
    with PostgresDB() as db:
        cursor = db.conn.cursor()

        # Iterate over features, add if not previously existing
        for cnt, quake in enumerate(geojson_dict['features']):

            # Add USGS ID Attribute
            quake['properties']['usgs_id'] = quake['id']

            # Read latitude and longitude from coodinates
            quake['properties']['longitude'] = \
                quake['geometry']['coordinates'][0]

            quake['properties']['latitude']= \
                quake['geometry']['coordinates'][1]

            # Add properties to earthquakes table
            p_keys = quake['properties'].keys()

            # Build sql for psycopg2 execute method
            placeholders = ', '.join(["%s" for _ in p_keys])
            sql = "INSERT INTO {table} ({columns}) " \
                  "VALUES ({values}) " \
                  "ON CONFLICT DO NOTHING;".format(table=table,
                                                   columns=', '.join(p_keys),
                                                   values=placeholders)

            values = [quake['properties'][k] for k in p_keys]

            # Insert record
            cursor.execute(sql, values)
            db.conn.commit()


def update_point_geom(table='earthquakes'):
    with PostgresDB() as db:

        sql = """UPDATE {table} SET geometry = ST_SetSRID(ST_MakePoint(
        {table}.longitude, {table}.latitude),4326)""".format(table=table)

        cursor = db.conn.cursor()
        cursor.execute(sql)

        # Commit and close cursor
        db.conn.commit()
        cursor.close()


def update_utc_time(table='earthquakes'):
    """Populates utc_time column based on time column"""
    with PostgresDB() as db:
        sql = """UPDATE {table} SET utc_time = to_timestamp({table}.time
        /1000);""".format(table={table})

        cursor = db.conn.cursor()
        cursor.execute(sql)

        # Commit and close cursor
        db.conn.commit()
        cursor.close()


def main():
    print('Reading data from USGS, inserting new records')
    update_usgs_data()
    print('   - updating point geom')
    update_point_geom()
    print('   - updating UTC time')
    update_utc_time()

if __name__ == '__main__':
    main()