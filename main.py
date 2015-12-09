#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division, print_function, absolute_import
import logging
import sqlite3
import traceback


class RecordAdapter(object):

    '''
    Adapter wrapping a log record object, and providing convenience methods
    '''

    def __init__(self, record):
        self.record = record

    @property
    def text(self):
        return self.record.getMessage()

    @property
    def traceback(self):
        if self.record.exc_info:
            return ''.join(traceback.format_tb(self.record.exc_info[-1]))

    def __getattr__(self, name):
        return getattr(self.record, name)

    def serialize(self):
        return {
            'name': self.name,
            'created': self.created,
            'level': self.levelname,
            'traceback': self.traceback,
            'text': self.text,
            'lineno': self.lineno,
            'function': self.funcName,
            'module': self.module,
            'pathname': self.pathname,
            'process': self.process,
            'thread': self.thread,
            'thread_name': self.threadName,
            'relative_created': self.relativeCreated,
        }


class DatabaseHandler(logging.Handler):

    '''Handles logging to database, with already open connection'''

    def __init__(self, connection, level=logging.NOTSET,
                 tablename='log_messages'):
        super(DatabaseHandler, self).__init__(level)
        self.connection = connection
        self.tablename = tablename
        self.initialise()

    def initialise(self):
        cursor = self.connection.cursor()
        cursor.execute('''create table if not exists {tablename} (
            name string not null,
            level string not null,
            created timestamp not null,
            lineno integer,
            traceback string,
            function string,
            module string,
            pathname string,
            process integer,
            thread long,
            thread_name string,
            relative_created float,
            text string
        )'''.format(tablename=self.tablename))
        cursor.execute('''create index if not exists level_index
                       on {tablename} (level)'''.format(
            tablename=self.tablename))
        cursor.close()

    def emit(self, record):
        out = RecordAdapter(record).serialize()
        self.write_entry(out)

    def write_entry(self, out):
        cursor = self.connection.cursor()
        query = '''insert into {tablename} ({keys})
                    values ({placeholders})'''.format(
            tablename=self.tablename,
            keys=','.join(out.keys()),
            placeholders=','.join(['?' for _ in out.keys()]))

        params = [out[key] for key in out.keys()]
        cursor.execute(query, params)
        try:
            self.connection.commit()
        except:
            self.connection.rollback()
        finally:
            cursor.close()


def main():
    logger = logging.getLogger(__name__)
    connection = sqlite3.connect(':memory:')
    handler = DatabaseHandler(connection, level=logging.DEBUG)
    logger.addHandler(handler)

    logger.setLevel('DEBUG')

    logger.info('This is a %s', 'test')
    try:
        raise RuntimeError("BAD")
    except RuntimeError:
        logger.exception('Bad')
    logger.critical('GREAT STUFF!')
    logger.debug('Debug stuff')

    # Fetch messages

    cursor = connection.cursor()
    cursor.execute('select * from log_messages')
    keys = [row[0] for row in cursor.description]

    for entry in cursor.fetchall():
        e = {a: b for (a, b) in zip(keys, entry)}
        print(e)

if __name__ == '__main__':
    main()
