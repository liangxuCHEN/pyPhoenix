# coding=utf-8
import phoenixdb


"""
使用phoenix，开启 queryserver.py start， 默认端口8765
pip install phoenixdb
"""
database_url = 'http://192.168.3.83:8765'


def get_row(table):
    """
    查询数据例子，execute里面直接放入sql 语句，可以通过多种方式返回结果
    :param table:
    :return:
    """
    conn = phoenixdb.connect(database_url, autocommit=True)
    # 安全方法，不需要手动close
    with conn.cursor() as cursor:
        # 每次请求返回数据量，默认是2000
        cursor.itersize = 10
        cursor.execute('select * from %s' % table)
        # 返回一条记录
        print cursor.fetchone()
        print cursor.next()
        # 返回多条记录
        print cursor.fetchmany(5)
        # 返回全部记录
        print cursor.fetchall()


def get_row_structure(table):
    """
    返回数据结构
    :param table:
    :return:
    """
    keys = list()
    conn = phoenixdb.connect(database_url, autocommit=True)
    with conn.cursor() as cursor:
        # 每次请求返回数据量，默认是2000
        cursor.itersize = 10
        cursor.execute('select * from %s' % table)

        for col in cursor.description:
            keys.append((col.name, col.t))

    return keys


def insert_data():
    """
    executemany 可以一次插入，更新多条语句
    :return:
    """
    conn = phoenixdb.connect(database_url, autocommit=True)
    with conn.cursor() as cursor:
        # 新建测试表格
        cursor.execute("DROP TABLE IF EXISTS test")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, text VARCHAR)")
        # 插入多条数据
        cursor.executemany("UPSERT INTO test VALUES (?, ?)", [[i, 'text {}'.format(i)] for i in range(10)])
        # 查询数据
        cursor.execute("SELECT * FROM test WHERE id>? ORDER BY id", [1])
        print cursor.fetchall()


if __name__ == '__main__':
    # get_row('test')
    # insert_data()
    print get_row_structure('test')
