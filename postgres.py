"""
Purpose: Postgres Database update and helper functions
Author: Conrad Hilley (conradhilley@gmail.com)
"""

from config import config
import json
import psycopg2
import usgs

# Commands used when updating gdb
UPDATE_CMDS = {'update_point_geom':
                   """UPDATE {table} SET geometry = ST_SetSRID(
                   ST_MakePoint({table}.longitude,
                   {table}.latitude), 4326);""",

               'update_utc_time':
                   """UPDATE {table} SET utc_time =
                   to_timestamp({table}.time/1000);"""}

# Commonly used sql commands
SQL_CMDS = {'estimate_row_count':
                """SELECT reltuples AS approximate_row_count FROM pg_class
                WHERE relname = '{table}';""",

            'count_rows':
                """SELECT count(*) FROM {table};"""
            }


class PostgresDB(object):
    def __init__(self, config_file='database.ini'):
        self.params = config(config_file=config_file, section='postgresql')
        self.conn = None

    def execute(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(**self.params)
            self.cursor = self.conn.cursor()
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
        with db.cursor as cursor:

            cursor.execute(SQL_CMDS['count_rows'].format(table='earthquakes'))
            init_cnt = cursor.fetchone()[0]

            # Iterate over features, add if not previously existing
            for cnt, quake in enumerate(geojson_dict['features']):
                # Add USGS ID Attribute
                quake['properties']['usgs_id'] = quake['id']

                # Read latitude and longitude from coodinates
                quake['properties']['longitude'] = \
                    quake['geometry']['coordinates'][0]

                quake['properties']['latitude'] = \
                    quake['geometry']['coordinates'][1]

                # Add properties to earthquakes table
                p_keys = quake['properties'].keys()

                # Build sql for psycopg2 execute method
                placeholders = ', '.join(["%s" for _ in p_keys])
                sql = "INSERT INTO {table} ({columns}) " \
                      "VALUES ({values}) " \
                      "ON CONFLICT DO NOTHING;".format(table=table,
                                                       columns=', '.join(
                                                           p_keys),
                                                       values=placeholders)

                values = [quake['properties'][k] for k in p_keys]

                # Insert record
                cursor.execute(sql, values)
                db.conn.commit()

            # Final Count
            cursor.execute(SQL_CMDS['count_rows'].format(table='earthquakes'))
            final_cnt = cursor.fetchone()[0]

            print('   - {} rows added ({} -> {})'.format(
                    final_cnt - init_cnt, init_cnt, final_cnt))


def main():

    print('Reading data from USGS, inserting new records')
    update_usgs_data()

    for cmd, sql in UPDATE_CMDS.items():
        print('\n    - {}'.format(cmd))
        with PostgresDB() as db:
            db.execute(sql.format(table='earthquakes'))
            db.conn.commit()


if __name__ == '__main__':
    main()
