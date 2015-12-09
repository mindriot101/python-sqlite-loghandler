import sqlite3
import pytest
import logging

from sqlitehandler import DatabaseHandler


@pytest.fixture(scope='module')
def connection():
    return sqlite3.connect(':memory:')


@pytest.fixture
def logger():
    l = logging.getLogger('sqlitehandler.testing')
    l.setLevel(logging.DEBUG)
    return l


def test_sqlitehandler(connection, logger):
    handler = DatabaseHandler(connection)
    logger.addHandler(handler)

    logger.info('Hello world')

    cursor = connection.cursor()
    cursor.execute('select count(*) from {}'.format(handler.tablename))
    nrows, = cursor.fetchone()
    assert nrows == 1
