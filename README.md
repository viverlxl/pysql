__python mysql类库__
- 支持sql注入
- 支持事务，支持读写锁


__如何使用__
```python
    """利用python setup.py工具装好之后"""
    from db import DataClient
    DATABASE = {
        "test":{
        "host": "localhost",
        "port": 3306,
        "username": "root",
        "password": "root",
        "schema"  : "test"
        }
    }
    dataClient = DataClient(DATABASE)
    test = dataClient.get_db_by_name("test")
    test.query(sql_statement)
```
- 详细使用可参考测试文档