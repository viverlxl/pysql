#### pysql

##### INSTALL

​	 python setup.py install 

#####IMPORT

​	from pysql import DataClient

#####USE

```python
from pysql import DataClient
DATABASE = {
  "test": {
    "host": "localhost",
    "port": 3306,
    "username": "root",
    "password": "",
    "schema": "test"
  },
  "test2": {
    "host": "localhost",
    "port": 3306,
    "username": "root",
    "password": "",
    "schema":"mysql",
  }
}
"""
	you can write databases config in your project setting file
	then in project you can:
		"app is your project"
		dataClient = DataClient(app.settings.DATABASE)
		for key, value in app.settings:
			setattr(app, key, dataClient.get_db_by_name(key))
	
	then you can get dataBase connect object like this:
		test = app.test
		test2 = app.test2
"""
dataClient = DataClient(DATABASE)
test = dataClient.get_db_by_name("test")
sql_statement = "select * from test"
data = test.query(sql_statement)
from item in data:
  print(item['name'])
  
 """
 		output:
 			里斯
      王五
      张三
      王五
      哈哈
 """
```

##### OTHER

if you need more 