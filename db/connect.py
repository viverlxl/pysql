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
    def __init__(self, conf = None, debug = False, insert_many_len = 100):
        if not isinstance(conf, dict):
            raise TypeError("conf require dict type ")
        self.conf = conf
        self._init_conf()
        self.debug = debug
        self.auto_commit = 1
        self.insert_many_len = insert_many_len
        self.update_many_len = 100
    
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
            self.auto_commit = 1
        except:
            self._reconn()
            self.cursor.execute("commit")
            self.auto_commit = 1

    def rollback(self):
        try:
            self.cursor.execute("rollback")
            self.auto_commit = 1
        except:
            self._reconn()
            self.cursor.execute("rollback")
            self.auto_commit = 1
        
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
        if not isinstance(entry_list, list) or not entry_list:
            raise TypeError("type error")
        fields = ", ".join(entry_list[0])
        
        def func(entry_list):
            statement =  str.format("({0})", ", ".join(str.format("\"{0}\"",self.sql_escape(entry_list[k])) if entry_list[k] is not None else "NULL" for k in entry_list))
            return statement

        def func2(table, fields, entry_list):
            statement = str.format("{3} {4} INTO {0} ({1}) VALUES {2}", table, fields, ', '.join(map(func, entry_list)), 
                                'INSERT' if not replace else 'REPLACE',
                                ' IGNORE ' if ignore else ' ')
            if self.debug:
                logging.info(statement)
            res = self.cursor.execute(statement)
            if self.auto_commit:
                self.commit()
            return res
        
        data_len = len(entry_list)
        insert_offset = 0
        column_num = 0
        while (data_len // self.insert_many_len):
            tmp_list = entry_list[insert_offset: (insert_offset + self.insert_many_len)]
            num = func2(table, fields, tmp_list)
            column_num += num
            data_len -= self.insert_many_len
            insert_offset += self.insert_many_len

        if data_len:
            final_list = entry_list[insert_offset:]
            tmp_num    = func2(table, fields, final_list)
            column_num += tmp_num
        return column_num
        

        

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

        values = ", ".join(str.format("{0}=\"{1}\"", k, self.sql_escape(entry[k])) for k in entry)
        conds  = " and ".join(str.format("{0}=\"{1}\"", k, self.sql_escape(cond[k])) for k in cond)
        statement = str.format("UPDATE {0} SET {1} WHERE {2}", table, values, conds)
        if self.debug:
            logging.info(statement)
        res = self.cursor.execute(statement)
        if self.auto_commit:
            self.commit()
        return res


    def update_many(self, entry, table, cond = {}):
        """
            entry: 更新的字段及更新条件
            tabe : 表名
        """
        if not entry:
            return None

        if not isinstance(entry, list) and not isinstance(cond, dict):
            raise TypeError("type error")

        if not entry[0].get("update_field", 0):
            raise KeyError("need update fields")

        def func(table, entry_list, cond):
            in_set = set()
            update_elem = []
            case_key = ""
            conds  = " and ".join(str.format("{0}=\"{1}\"", k, self.sql_escape(cond[k])) for k in cond)

            for key in entry_list[0]:
                try:
                    case_key = list(entry_list[0]['update_field'].keys())[0]
                except:
                    return
                
                update_str_list = []
                if key != "update_field":
                    for each in entry_list:
                        val = self.sql_escape(each[key])
                        case_val = self.sql_escape(each['update_field'][case_key])
                        in_set.add(case_val)
                        update_str = str.format("WHEN \"{0}\" THEN \"{1}\"",case_val, val)
                        update_str_list.append(update_str)
                
                tmp_str = "{0}= CASE {1} {2}".format(key, case_key, " ".join(update_str_list))
                
                if key == "update_field":
                    continue
                update_elem.append(tmp_str)
            if not update_elem:
                return
            statement = "UPDATE {0} SET {1} END WHERE {2} IN {3} AND {4}".format(table, " END, ".join(update_elem), case_key, tuple(in_set), conds if conds else " 1 = 1")

            if self.debug:
                logging.info(statement)
            res = self.cursor.execute(statement)
            if self.auto_commit:
                self.commit()
            return res


        data_len = len(entry)
        update_offset = 0
        column_num = 0
        while (data_len // self.update_many_len):
            tmp_list = entry[update_offset: (update_offset + self.update_many_len)]
            num      = func(table, tmp_list, cond)
            column_num += num
            data_len -= self.update_many_len
            update_offset += self.update_many_len

        if data_len:
            final_list = entry[update_offset:]
            tmp_num    = func(table, final_list, cond)
            column_num += tmp_num
        return column_num
    


    def query(self, statement, use_result = False):
        data = None
        try:
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
        self.commit()
        fields = []
        for col_name in cols:
            fields.append(col_name[0])
        result = []           
        for row_data in data:
            tmp = dict(zip(fields, row_data))
            result.append(tmp)
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
   
    




    

    
        
    



    


    




