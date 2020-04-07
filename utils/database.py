import pymysql

from config import DATABASES


class RDB:
    def __init__(self, alias, **kwargs):
        try:
            self.config = DATABASES[alias]
            self.engine = self.config['ENGINE']

            # NOTE: pymysql compatible
            if self.config.get('SSL_REQUIRED') is True:
                kwargs['ssl'] = {'ssl': {'ca': self.config['SSL_CA']}}

            if 'mysql' in self.engine:
                self.conn = pymysql.connect(
                    database=self.config['NAME'],
                    host=self.config['HOST'],
                    user=self.config['USER'],
                    password=self.config['PASSWORD'],
                    charset='utf8mb4',  # NOTE: hardcoded variable
                    autocommit=False,
                    **kwargs,
                )

            else:
                raise NotImplementedError

        except KeyError:
            raise

    def __del__(self):
        self.close()

    def _cursor(self, **kwargs):
        if not self.conn:
            raise RuntimeError('no connection')

        # get `cursor` keyword argument, set default to `dict`
        cursor_type = kwargs.get('cursor', 'dict')

        if cursor_type in ('tuple', 'default'):
            cursor = self.conn.cursor()

        # elif cursor_type == 'dict':
        else:
            if self.engine == 'mysql':
                cursor = self.conn.cursor(pymysql.cursors.DictCursor)

            elif self.engine == 'mssql':
                cursor = self.conn.cursor(as_dict=True)

        return cursor

    def execute(self, query, params=None, *args, **kwargs):
        cursor = None

        try:
            cursor = self._cursor(**kwargs)

            # CHANGED: always perform executemany
            # if kwargs.get('many') is True:
            #     res = cursor.executemany(query, params)
            # else:
            #     res = cursor.execute(query, params)
            res = cursor.executemany(query, params)

            if kwargs.get('commit') is not False:
                self.conn.commit()

        except Exception as e:
            # TODO: DB ROLLBACK
            raise

        finally:
            if cursor:
                cursor.close()

        return res

    def fetch(self, query, params=None, *args, **kwargs):
        cursor = None

        try:
            cursor = self._cursor(**kwargs)

            cursor.execute(query, params)

            if kwargs.get('one'):
                res = cursor.fetchone()

            elif kwargs.get('limit'):
                res = cursor.fetchmany(size=kwargs['limit'])

            else:
                res = cursor.fetchall()

            if kwargs.get('single'):
                res = res[0]

        except Exception as e:
            raise

        finally:
            if cursor:
                cursor.close()

        return res

    def close(self):
        try:
            self.conn.close()
        except:
            pass
