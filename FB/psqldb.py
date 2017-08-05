from lazy import lazy
import numpy as np
import pandas as pd
import psycopg2


class PsqlDB():
    def __init__(self, **kwargs ):
        super().__init__()
        self.db_name = kwargs.get('db_name', 'postgres')
        self.user = kwargs.get('user', 'postgres')
        self.host = kwargs.get('host', 'localhost')
        self.password = kwargs.get('host', 'postgres')


    @lazy
    def conn(self):
        try:
            return psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(self.db_name, self.user, self.host, self.password))
        except:
            return None


    @lazy
    def cursor(self):
        if ( self.conn is None ):
            return None

        return self.conn.cursor()


    @lazy
    def column_names(self):
        return [row[0] for row in self.cursor.description]


    def drop_table(self, table_name):
        if ( self.is_table_exists(table_name=table_name) ):
            return self.prepare_and_execute('DROP TABLE {};'.format(table_name))

        return 1


    def select(self, table_name=None):
        if ( table_name is None ): return []

        return self.query("select * from %s" % (table_name))


    def select_df(self, table_name=None):
        return pd.DataFrame(self.select(table_name=table_name))


    def query(self, query_str=None):
        if ( query_str is None ): return []

        self.cursor.execute(query_str)
        return [dict(zip(self.column_names,r)) for r in self.cursor.fetchall()]


    def insert(self, table_name='', data={}):
        sql, params = self.get_insert_sql_and_params(table_name=table_name, data=data)
        return self.prepare_and_execute(sql, params)


    def delete(self, table_name='', where={}):
        sql, params = self.get_delete_sql_and_params(table_name=table_name, where=where)
        return self.prepare_and_execute(sql, params)


    def get_insert_sql_and_params(self, table_name='', data={}):
        myDict = self.filter_field_vals(table_name=table_name, data=data)

        placeholders = ', '.join(['%s'] * len(myDict))
        fields = myDict.keys()
        columns = ', '.join(fields)
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table_name, columns, placeholders)
        params = []
        for field in fields:
            params.append(myDict[field])

        return [ sql, params ]



    def get_delete_sql_and_params(self, table_name='', where={}):
        return self.get_sql_and_params(table_name=table_name, where=where, sql_type='delete')

         
    def filter_field_vals(self, table_name='', data={}):
        table_fields = self.get_table_fields(table_name)
        new_data = {}
        for key in data.keys():
            if key in table_fields:
                new_data[key] = data[key]
        return new_data        


    def get_table_fields(self, table_name=''):
        if ( table_name == '' ): return []

        self.cursor.execute("select column_name from information_schema.columns where table_name = '%s'" % (table_name))

        return [r[0] for r in self.cursor.fetchall()]


    def get_sql_and_params(self, table_name='', where={}, sql_type=None):
        if ( sql_type == None or re.search('(select|delete)', sql_type) == None):
            return ['', []]

        fields = where.keys()
        if ( len(fields) == 0 ):
            if ( re.search('select', sql_type) ):
                return ['SELECT * from %s ' % (table_name), []]
            return ['', []]

        sql = 'SELECT * ' if ( sql_type == 'select' ) else 'DELETE '

        sql = sql + "FROM %s WHERE " % (table_name)
        params = []
        for field in fields:
            sql = sql + field + ' = %s AND '
            params.append(where[field])

        sql = re.sub("AND $", "", sql)    
        return [ sql, params ]


    def prepare_and_execute(self, sql='', params=[]):
        rs = ''
        try:
            self.cursor.execute(sql, params)
            #self.cursor.close()
            self.conn.commit()
            rs = 1

        except (Exception, psycopg2.DatabaseError) as error:
            rs = error

        finally:
            return rs    
#             if self.conn is not None:
#                 self.conn.close()

        
    def is_table_exists(self, table_name=None):
        if ( table_name is None ): return False

        self.cursor.execute("select * from information_schema.tables where table_name=%s", [table_name])

        return bool(self.cursor.rowcount)


