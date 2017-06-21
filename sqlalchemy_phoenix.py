# coding=utf-8
import sqlalchemy

"""
使用phoenix，开启 queryserver.py start， 默认端口8765
pip install SQLAlchemy pyPhoenix
"""
database_url = '192.168.3.83:8765'


def get_row(table):
    engine = sqlalchemy.create_engine('phoenix://%s/' % database_url)
    with engine.connect() as connection:
        res = connection.execute('select * from %s' % table)
        # 返回一条记录
        print res.fetchone()
        # 返回多条记录
        print res.fetchmany(5)
        # 返回全部记录
        print res.fetchall()


if __name__ == '__main__':
    get_row('test')
