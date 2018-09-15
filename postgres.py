"""
Purpose: Postgres Database update and helper functions
Author: Conrad Hilley (conradhilley@gmail.com)
"""

# TODO create table/cursor classes and add common commands as methods
# TODO build base cursor class

from config import config
import json
import psycopg2
import psycopg2.extras
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
    def __init__(self, config_file='database.ini', section='postgresql'):
        self.params = config(config_file=config_file, section=section)
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


class SearchCursor(object):

    def __init__(self, conn, table, columns=(), query=None, chunk_size=100000,
                 as_dict=False):
        self.as_dict = as_dict
        self.chunk_size = chunk_size
        self.cursor = conn.cursor(
                cursor_factory=psycopg2.extras.NamedTupleCursor)

        # table and column validation
        self.table = table
        self.search_cols = columns
        self.avail_cols = self._columns()

        # verify column type and presence
        if self.search_cols:

            if isinstance(self.search_cols, str):
                self.search_cols = (self.search_cols, )

            for col in self.search_cols:
                if col not in self.avail_cols:
                    raise KeyError('Column ({}) not in {}'.format(col,
                                                                  self.table))
        else:
            self.search_cols = ('*', )

        # build sql
        self.sql = """SELECT {cols} from {table}""".format(
                cols=', '.join(map(str, self.search_cols)),
                table=self.table)

        # add query
        self.query = query
        if self.query:
            self.sql = '{sql} WHERE {query};'.format(sql=self.sql,
                                                    query=self.query)
        else:
            self.sql += ';'

    def _columns(self):
        try:
            self.cursor.execute("SELECT * FROM {table} LIMIT 0".format(
                    table=self.table))
            return [desc[0] for desc in self.cursor.description]
        except:
            raise KeyError('Table ({}) not in database'.format(self.table))

    def __iter__(self):
        while True:
            self.records = self.cursor.fetchmany(self.chunk_size)
            if not self.records:
                break
            for rec in self.records:

                # If all records yield as dict or named tuple
                if self.search_cols == ('*', ):
                    if self.as_dict:
                        yield rec._asdict()
                    else:
                        yield rec
                else:
                    # Will need dict for all other access methods
                    rec_dict = rec._asdict()

                    if self.as_dict:
                        yield  rec_dict
                    else:
                        yield tuple([rec_dict[c] for c in self.search_cols])


    def __enter__(self):
        try:
            self.cursor.execute(self.sql)
            self.records = None
        except ConnectionError('Invalid cursor parameters'):
            pass
        finally:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()


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
            with db.cursor as cursor:
                cursor.execute(sql.format(table='earthquakes'))
                db.conn.commit()


if __name__ == '__main__':
    main()
