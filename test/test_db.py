# coding:utf-8
import pytest

from db import DataClient

test = ""

# class Test:
DATABASE = {
        "test":{
        "host": "localhost",
        "port": 3306,
        "username": "root",
        "password": "",
        "schema"  : "test"
        }
    }
DEBUG = True




def setup_function():
    global test
    dataClient = DataClient(DATABASE)
    print("初始化数据库")
    test = dataClient.get_db_by_name("test")



def teardown_function():
    global test
    print("关闭数据库")
    test.close()


def test_query_one():
    global test
    sql = """
        select * from test limit 1
    """
    data = test.query(sql)
    print("单查询测试%s"%data['name'])

def test_query():
    global test
    sql = """
        select * from test limit 10
    """
    data = test.query(sql)
    for item in data:
        print(item['id'])


def test_insert():
    entity = {
                "name": "张三",
                "age" : 10
                }
    entity2 = {
            "name": "王五",
            "age" : 20
    }
    num = test.insert(entity, "test")
    num = test.insert(entity2, "test")
    print("insert 测试%s"%num)

def test_update():
    global test
    update_entry = {
            "age" : "4",
            "name": "李四"
        }
    cond = {
            "id": "2"
        }
    num = test.update(update_entry, "test", cond)
    print("update 测试 %s"%num)


def test_delete():
    global test
    cond = {
            "id": "1"
        }
    num = test.delete("test", cond)
    print("delete 测试%s"%num)



if __name__ == "__main__":
    pytest.main(["-s","test_db.py"])
