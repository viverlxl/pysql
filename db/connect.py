#coding:utf-8
import pymysql
import logging
import datetime


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)

class DataBase:
    LOCK_READ  = 0
    LOCK_WRITE = 1
    def __init__(self, conf = None, debug = False):
        if not isinstance(conf, dict):
            raise TypeError("conf require dict type ")
        self.conf = conf
        self._init_conf()
        self.debug = debug
        self.auto_commit = 1
    
    def _init_conf(self):
        conf = self.conf.copy()
        self.db, self.cursor = self._conn(conf)
       

    def _conn(self, spec):
        try:
            db = pymysql.connect(host = spec["host"], port = spec["port"], user=spec["username"],
                                passwd = spec["password"], db=spec["schema"])
            cursor = db.cursor()
            cursor.execute("SET NAMES 'utf8mb4'")
        except Exception:
            raise Exception("import mysql error")
        return db, cursor


    def _reconn(self):
        self._conn(self.conf)


    def lock(self, tbl_list, lock_type = LOCK_WRITE):
        sql = ", ".join(
            "%s %s"%(tbl, "read local" if lock_type == self.LOCK_READ else "write local") for tbl in tbl_list)
        statement = "LOCAK TABLES {0}".format(sql)
        if self.debug:
            logging.info(statement)
        self.cursor.execute(statement)

    def unlock(self):
        self.cursor.execute("unlock tables")

    def start_transaction(self):
        try:
            self.cursor.execute("start transaction")
            self.auto_commit = 0
        except:
            self._reconn()
            self.cursor.execute("start transaction")
            self.auto_commit = 0

    def commit(self):
        try:
            self.cursor.execute("commit")
        except:
            self._reconn()
            self.cursor.execute("commit")

    def rollback(self):
        try:
            self.cursor.execute("rollback")
        except:
            self._reconn()
            self.cursor.execute("rollback")
        
    def close(self):
        self.db.close()

    def sql_escape(self, statement):
        if not isinstance(statement,str):
            return statement
        result = pymysql.escape_string(statement)
        return result


    def insert(self, entry, table, ignore = False, replace = False):
        """
        """
        entry_list = [entry] if isinstance(entry, dict) else entry
        if not isinstance(entry_list, list):
            raise TypeError("type error")
        fields = ", ".join(entry_list[0])
        
        def func(entry_list):
            statement =  str.format("({0})", ", ".join(str.format("\"{0}\"",self.sql_escape(entry_list[k])) if entry_list[k] is not None else "NULL" for k in entry_list))
            return statement

        statement = str.format("{3} {4} INTO {0} ({1}) VALUES {2}", table, fields, ', '.join(map(func, entry_list)), 
                            'INSERT' if not replace else 'REPLACE',
                            ' IGNORE ' if ignore else ' ')
        if self.debug:
            logging.info(statement)
        res = self.cursor.execute(statement)
        if self.auto_commit:
            self.commit()
        return res

    def delete(self,table, cond):
        """
        table: 表名
        cond : where 条件
        """
        if not isinstance(cond, dict):
            raise TypeError("type erro")

        conds  = ", ".join(str.format("{0}=\"{1}\"", k, cond[k]) if cond[k] else 'NULL' for k in cond)
        statement = str.format("DELETE FROM  {0} WHERE {1}", table, conds)
        if self.debug:
            logging.info(statement)
        res = self.cursor.execute(statement)
        if self.auto_commit:
            self.commit()
        return res

        

    def update(self, entry, table, cond):
        """
        entry: 需要更新字段
        table: 表名
        cond : where 条件
        """
        if not isinstance(entry, dict) and not isinstance(cond, dict):
            raise TypeError("type error")

        values = ", ".join(str.format("{0}=\"{1}\"", k, entry[k]) if entry[k] else 'NULL' for k in entry)
        conds  = ", ".join(str.format("{0}=\"{1}\"", k, cond[k]) if cond[k] else 'NULL' for k in cond)
        
        statement = str.format("UPDATE {0} SET {1} WHERE {2}", table, values, conds)
        if self.debug:
            logging.info(statement)
        res = self.cursor.execute(statement)
        if self.auto_commit:
            self.commit()
        return res

    def query(self, statement, use_result = False):
        data = None
        try:
            self.cursor.execute(statement)
            data = self.execute_query(statement)
        except:
            self._reconn()
            data = self.execute_query(statement)
        if self.debug:
            logging.info(statement)
        return data 
    
    
    def execute_query(self, statement):
        self.cursor.execute(statement)
        cols = self.cursor.description
        data = self.cursor.fetchall()
        fields = []
        for col_name in cols:
            fields.append(col_name[0])
        result = []           
        for row_data in data:
            tmp = dict(zip(fields, row_data))
            result.append(tmp)
        if len(result) == 1:
            return result[0]
        return result

    def execute(self, statement):
        if self.debug:
            logging.info(statement)
        res = self.cursor.execute(statement)
        if self.auto_commit:
            self.commit()
        return res



class DataClient:
    """
    连接端
    """
    def __init__(self, conf, debug = False):
        self._init_conf(conf, debug)
        self.conns = {}
    
    def _init_conf(self, conf, debug):
        if isinstance(conf, dict):
            self.conf = conf
            self.debug = debug
        else:
            try:
                self.conf = conf.DATABASE
            except:
                raise AttributeError("can not get DATABASE ")
            try:
                self.debug = conf.DEBUG
            except:
                self.debug = debug
        
    
    def get_db_by_name(self, db_name):
        """
        db_name: 需要连接的数据库, 第一次引入时需要显示get
        """            
        db = self.conns.get(db_name, None)
        if not db:
            if db_name in self.conf:
                db = DataBase(self.conf[db_name], debug = self.debug)
                self.conns.setdefault(db_name, db)
            else:
                raise NameError("db name error")
        return db
   
    




    

    
        
    



    


    




