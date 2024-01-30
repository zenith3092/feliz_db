import psycopg2  
import logging, traceback
from enum import Enum

class db_operation_mode(Enum):
    """
    Enum to define the mode of operations
    
    Args:
        mode = 0 -> normal execute
        mode = 1 -> execute with arguments
        mode = 2 -> with return values but without arguments
        mode = 3 -> with return values and with arguments
    """
    MODE_DB_NORMAL = 0
    MODE_DB_W_ARGS = 1
    MODE_DB_W_RETURN_WO_ARGS = 2
    MODE_DB_W_RETURN_AND_ARGS = 3

class PostgresHandler:
    def __init__(self, host, port, database, username, password):
        self.db_type = "postgres"
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

        self.table_header_dict = {}

        # init logger if there is no logger init
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(level=logging.INFO)

        # test connection
        conn = None
        try:
            conn = psycopg2.connect(database=self.database, user=self.username, password=self.password, host=self.host, port=self.port, connect_timeout=5)
            logging.info(" [PostgresHandler] Connection with database is OK")
        except Exception as e:
            logging.warning(" [PostgresHandler] Cannot connect to database server, because "+str(e))
        finally:
            if conn is not None:
                conn.close()

    def _execute_sql(self, mode, sql, entries=[], multiple=False, statement_timeout=-1):
        """
        This is the functiino to execute sql command.
        If there are many data to insert/update, let the type of entries be list of list of args and set the param multiple to True,
        and then executemany will be called. 

        Args:
            mode (db_operation_mode type): type to execute
            sql (string): The SQL command to execute
            entries (list of args OR list of list of args, optional): The SQL args to execute. Defaults to [].
                type of entries == list of args means the sql only insert/update one data. => use execute(sql, entries)
                type of entries == list of list of args means the sql insert/update MORE than one data. => use executemany(sql, entries)
            multiple (boolean, optinal): If the entries have multiple data to execute. Defaults to False.
            statement_timeout (int, optional): Timeout number in second for statement. -1 means no timeout. Defaults to -1.

        Returns:
            result (dictionary):
                - indicator (boolean): Show the result of this execution
                - message (string): Message to show. Mainly for error message.
                - header (list of string): The header list of output data. Empty list if the execution has no return.
                - data (list of tuple): The return data from database. Empty list if the execution has no return.
                - formatted_data (list of dictionary): The return data from database. Empty list if the execution has no return.
        """
        result = {
            "indicator": False,
            "message": "",
            "header": [],
            "data": [],
            "formatted_data": []
        }
        conn = None

        try:
            if statement_timeout <= 0:
                conn = psycopg2.connect(database=self.database, user=self.username, password=self.password, host=self.host, port=self.port, connect_timeout=5)
            else:
                timeout_arg = '-c statement_timeout='+str(statement_timeout*1000) # options for statement timeout
                conn = psycopg2.connect(database=self.database, user=self.username, password=self.password, host=self.host, port=self.port, connect_timeout=5, options=timeout_arg)
            c = conn.cursor()

            if mode == db_operation_mode.MODE_DB_NORMAL:
                c.execute(sql)
            elif mode == db_operation_mode.MODE_DB_W_ARGS:
                if multiple:
                    c.executemany(sql, entries)
                else:
                    c.execute(sql, entries)
            elif mode == db_operation_mode.MODE_DB_W_RETURN_WO_ARGS:
                c.execute(sql)
                result["data"] = c.fetchall()
                result["header"] = [description[0] for description in c.description]
                result["formatted_data"] = [{result["header"][i]: value for i, value in enumerate(row)} for row in result["data"]]
            elif mode == db_operation_mode.MODE_DB_W_RETURN_AND_ARGS:
                if multiple:
                    c.executemany(sql, entries)
                else:
                    c.execute(sql, entries)
                result["data"] = c.fetchall()
                result["header"] = [description[0] for description in c.description]
                result["formatted_data"] = [{result["header"][i]: value for i, value in enumerate(row)} for row in result["data"]]
            else:
                raise Exception("Invalid mode")

            conn.commit()
            c.close()

            result["indicator"] = True
            result["message"] = "operation succeed"
        except Exception as e:
            traceback.print_exc()
            logging.warning(("[PostgresHandler] execute_sql Error: "+str(e)))
            result["message"] = str(e)
        finally:
            if conn is not None:
                conn.close()

        return result

    def get_table_list(self):
        """
        Get the table list from database.
        Returns:
            table_list: (list of string) the table list.
        """
        sql_cmd = "SELECT pg_tables.tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
        result = self._execute_sql(db_operation_mode.MODE_DB_W_RETURN_WO_ARGS, sql_cmd)
        table_list = []
        for t in result["data"]:
            table_list.append(t[0])
        return table_list

    def get_headers(self, table_name, force=False, no_ser_pk=False):
        """
        If the table_name has been in self.table_header_dict already, get the header list from self.table_header_dict.
        If not, execute sql command to get the header and store it in self.table_header_dict.
        
        Args:
            table_name (string): The table name to get column. If you need to specify the schema, you could let table_name="[your_schema].[your_table]"
            force (boolean, optional): Defaults to False.
                Set to True if you want to get the header by executing sql command.
            no_ser_pk (boolean, optional): Defaults to False.
                Set to True if you want to get the header without serial primary key.
        Returns:
            header_list: (list of string) the table's header.
        """
        if table_name in self.table_header_dict and not force:
            return self.table_header_dict[table_name]
        else:
            table_info = table_name.split(".")
            if len(table_info) >= 2:
                schema, table = table_info[0], table_info[1]
            else:
                schema, table = 'public', table_name
            sql_cmd = "SELECT column_name FROM information_schema.columns"
            if no_ser_pk:
                sql_cmd += " WHERE (table_schema = '{}') AND (table_name = '{}') AND (column_default NOT LIKE 'nextval%' OR column_default IS NULL)".format(schema, table)
            result = self._execute_sql(db_operation_mode.MODE_DB_W_RETURN_WO_ARGS, "{};".format(sql_cmd))
            return [item[0] for item in result["data"]]

    def get_data(self, table, target_column_list=[], conditional_rule_list=[], order_by_list=[], limit_number=-1):
        """
        Args:
            - table (string): The name of the target table.
            - target_column_list (list of string, optional): Defaults to [].
                The list of column names whose data we want to get from database.
                The list could get with get_headers.
                If this is an empty list, select all columns.
            - conditional_rule_list (list of tuple, optional): Defaults to []. 
                The condition for SELECT. The input should be like [("_id>", 1), ...]
                The first element of tuple is the column name and the operator, 
                the operator could be one of [=, <, >, <=, >=, LIKE].
                The second element is the target value which could be in basic python datatype, it should contains '%' if you use LIKE operator.
                e.g. [("_id=", 1)], [("camera_ip LIKE", "192.168%"), ("number<", 100)]
                ** There must be a blank before LIKE **
            - order_by_list (list of string, optional): Defaults to [].
                The order statement. The input should be like ["_id DESC", ...]
                The string contains 2 part. The first part is the column ordered by.
                The second part is the rule, which could be "DESC" or "ASC"
            - limit_number (int, optional): Defaults to -1.
                The limit number of output data. -1 means no limit

        Returns:
            result: result from _execute_sql without header and data
        """
        try:
            sql_cmd = "SELECT "
            if len(target_column_list) == 0:
                sql_cmd += "* FROM {} ".format(table)
            else:
                for column_name in target_column_list:
                    sql_cmd += "{}, ".format(column_name)
                sql_cmd = sql_cmd[:-2]+" FROM {} ".format(table) # remove the last ',' and add ' FROM '

            entries = []
            if len(conditional_rule_list) > 0:
                sql_cmd += "WHERE "
                for condition_rule in conditional_rule_list:
                    sql_cmd += "{} %s AND ".format(condition_rule[0])
                    entries.append(condition_rule[1])
                sql_cmd = sql_cmd[:-4]

            if len(order_by_list) > 0:
                sql_cmd += "ORDER BY "
                for order_by_rule in order_by_list:
                    sql_cmd += "{}, ".format(order_by_rule)
                sql_cmd = sql_cmd[:-2]+" "
            
            if limit_number >= 0:
                sql_cmd += "LIMIT {} ".format(limit_number)

            sql_cmd = sql_cmd[:-1]+";"
            logging.debug("[PostgresHandler] get_data sql_cmd: %s, entries: %s"%(sql_cmd, str(entries)))
            if entries == []:
                # No ARGS
                result = self._execute_sql(db_operation_mode.MODE_DB_W_RETURN_WO_ARGS, sql_cmd)
            else:
                # With ARGS
                result = self._execute_sql(db_operation_mode.MODE_DB_W_RETURN_AND_ARGS, sql_cmd, entries)
            
            return result
        except Exception as e:
            msg = "[PostgresHandler] get_data ERROR: "+str(e)
            logging.error(msg)
            return {"indicator": False, "message":msg, "header":[], "data":[], "formatted_data": []}
        
    def update_data(self, table, editing_list, reference_column_list):
        """
        Example:
                self.update_data("unique_test", [{"_id":1, "roi_id":2}], ["_id"]) 
                    => search the row(s) whose _id=1, edit it(them) to _id=1, roi_id=2
                self.update_data("unique_test", [{"_id":1, "roi_id":2}, {"_id":2, "roi_id":3}], ["_id"])
                    => search the row(s) whose _id=1, edit it(them) to _id=1, roi_id=2   
                            and the row(s) whose _id=2, edit it(them) to _id=2, roi_id=3
                self.update_data("unique_test", [{"_id":1, "roi_id":2, camera_ip:"192.168.1.179"}, {"_id":2, "roi_id":3, camera_ip:"192.168.1.80"}], ["roi_id", "camera_ip"])
                    => search the row(s) whose roi_id=2 AND camera_ip=192.168.1.179, edit it(them) to _id=1, roi_id=2, camera_ip=192.168.1.179
                            and the row(s) whose roi_id=3 AND camera_ip=192.168.1.80, edit it(them) to _id=2, roi_id=3, camera_ip=192.168.1.80

        Args:
            table (string): The name of the target table.
            editing_list (list of dictionary): The data wanted to be update. The format should be like [{"roi_id": 2, "camera_ip":"10.0.0.12"}, ...]
                The key of the dictionary is the column names.
                All of the dictionary in editing_list has to contain all of the key in reference_column_list.
            reference_column_list (list of string): Indicate which columns are for reference. The format should be like ["_id", ...]

        Returns:
            result (dictionary):
                - indicator (boolean)
                - message (string) 
        """
        if editing_list == []:
            return {"indicator": True, "message": "Nothing to update."}
        if reference_column_list == []:
            return {"indicator": False, "message": "No reference."}
        if not all([set(reference_column_list).issubset(list(data.keys())) for data in editing_list]):
            return {"indicator": False, "message": "Mismatch between reference_column_list and editing_list."}
        
        try:
            sql_cmd = "UPDATE {} SET ".format(table)
            entries = []
            generate_sql_set_part = False
            for data in editing_list:
                entries.append([])
                for key in data:
                    entries[-1].append(data[key])
                    if not generate_sql_set_part:
                        sql_cmd += "{}=%s, ".format(key)
                for column in reference_column_list:
                    entries[-1].append(data[column])
                generate_sql_set_part = True
            sql_cmd = sql_cmd[:-2]+" "

            sql_cmd += "WHERE "
            for column in reference_column_list:
                sql_cmd += "{}=%s AND ".format(column)
            sql_cmd = sql_cmd[:-4]+";"

            logging.debug("[PostgresHandler] update_data sql_cmd: %s, entries: %s"%(sql_cmd, str(entries)))
            result = self._execute_sql(db_operation_mode.MODE_DB_W_ARGS, sql_cmd, entries, multiple=True)
            del result["header"]
            del result["data"]
            return result
        except Exception as e:
            msg = "[PostgresHandler] update_data ERROR: "+str(e)
            logging.error(msg)
            return {"indicator": False, "message":msg}

    def add_data(self, table, adding_list, adding_header_list=[], to_null=False, no_ser_pk=True):
        """
        Args:
            table (string): The name of the target table.
            adding_list (list of dictionary): Data list to add. The format should be like [{"camera_ip":"192.168.1.179", "roi_id":12}, ...]
                The key of dictionaries is the column name of the data.
            adding_header_list (list of string, optional): Defaults to []. The column list to be added. The format should be like ["camera_ip", "roi_id"]
                This means that we only wants to add ths data of camera_ip column and roi_id camera_ip to the table.
                If this is an empty list, will get the complete header list from self.get_headers().
            to_null (boolean, optional): Defaults to False. If to_null == True, we will set the lacked data to null.
                For example, adding_header_list=["camera_ip", "roi_id"], adding_list=[{"camera_ip":"123.456.789.0"}], then the target value of "roi_id" will be null(None).
            no_ser_pk (boolean, optional): Defaults to True. If no_ser_pk == True, we will not add the serial primary key.

        Returns:
            result: result from _execute_sql without header and data
        """
        if adding_list == []:
            return {"indicator": True, "message": "Nothing to add."}
        if adding_header_list == []:
            adding_header_list = self.get_headers(table, no_ser_pk=no_ser_pk)

        try:
            column_name_cmd = ""
            value_cmd = ""
            for header in adding_header_list:
                column_name_cmd += "{}, ".format(header)
                value_cmd += "%s, "
            column_name_cmd = column_name_cmd[:-2]
            value_cmd = value_cmd[:-2]

            sql_cmd = "INSERT INTO {} ({}) VALUES ({});".format(table, column_name_cmd, value_cmd)
            entries = []

            for data in adding_list:
                entries.append([])
                for key in adding_header_list:
                    if key in data:
                        entries[-1].append(data[key])
                    elif to_null:
                        entries[-1].append(None)
                    else:
                        raise Exception("Lack of Data ( {} ): {}".format(table, key))
            logging.debug("[PostgresHandler] add_data sql_cmd: %s, entries: %s"%(sql_cmd, str(entries)))
            result = self._execute_sql(db_operation_mode.MODE_DB_W_ARGS, sql_cmd, entries, multiple=True)
            del result["header"]
            del result["data"]
            return result
        except Exception as e:
            msg = "[PostgresHandler] add_data ERROR: "+str(e)
            logging.error(msg)
            return {"indicator": False, "message":msg}

    def delete_data(self, table, filter_list, reference_column_list):
        """
        Args:
            table (string): The name of the target table.
            filter_list (list of dictionary): The filter list to indicate which columns to be deleted.
                The format should be like [{"username":"linga", "lastupdatetime":456789}, {"username":"linga", "lastupdatetime":123456}, ...]
                All of the dictionary in filter_list has to contain all of the key in reference_column_list.
            reference_column_list (list of string): Indicate which columns are for reference. The format should be like ["_id", ...]
                
        Returns:
            result: result from _execute_sql without header and data
        """
        if filter_list == []:
            return {"indicator": True, "message": "Nothing to delete."}
        if reference_column_list == []:
            return {"indicator": False, "message": "No reference."}
        if not all([set(reference_column_list).issubset(list(data.keys())) for data in filter_list]):
            return {"indicator": False, "message": "Mismatch between reference_column_list and filter_list."}
        
        try:
            sql_cmd = "DELETE FROM {} WHERE ".format(table)
            entries = []
            for column in reference_column_list:
                sql_cmd += "{}=%s AND ".format(column)
            sql_cmd = sql_cmd[:-4]+";"

            for data in filter_list:
                entries.append([])
                for column in reference_column_list:
                    entries[-1].append(data[column])
            logging.debug("[PostgresHandler] delete_data sql_cmd: %s, entries: %s"%(sql_cmd, str(entries)))
            result = self._execute_sql(db_operation_mode.MODE_DB_W_ARGS, sql_cmd, entries, multiple=True)
            del result["header"]
            del result["data"]
            return result
        except Exception as e:
            msg = "[PostgresHandler] delete_data ERROR: "+str(e)
            logging.error(msg)
            return {"indicator": False, "message":msg}

class PostgresModelHandler():
    """
    This is the class to define the postgres model.

    Required Class Attributes:
        meta (dict): The meta data of this class.
            \- initialize (boolean): If this class will be initialized when server starts.
            \- conditional_init (boolean): If this class will be initialized conditionally.
            \- authorization (string): The authorization of the schema. Required when meta["init_type"] == "schema".
            \- init_type (string): The type of initialization. It could be "schema" or "table".
            \- schema_name (list): The schema name.
            \- table_name (list): The table name, but the length of this list should not be greater than 1.
    
    Required Class Methods (when meta["conditional_init"] == True):
        if_initialize (method): The method to form the conditional initialization sql command.
        else_initialize (method): The method to form the conditional initialization sql command.
    
    Limitation:
        \- The attribute name of the concrete class should not start with "_"
    
    The attributes of the concrete class which is inherited from this class:
        _table_name (string): The table name.
        _headers (list): The headers of the table.
        _required_headers (list): The required headers of the table.
        _headers_type (dict): The headers type of the table.
        _headers_default (dict): The headers default value of the table.
        _field_dict (dict): The field dictionary.
        _field_conditions (string): The field conditions (sql conditions).
        _field_index_dict (dict): The field index dictionary.
        other attributes: Named by the headers of the table and the value is the default value of the table.
    """
    SCHEMA = "schema"
    TABLE = "table"
    META_REQUIRED = ["initialize", "conditional_init", "init_type"]
    TABLE_META_REQUIRED = ["schema_name", "table_name"]
    SCHEMA_META_REQUIRED = ["schema_name", "authorization"]
    CONDITIONAL_INIT_REQUIRED = ["if_initialize", "else_initialize"]

    _table_entries_dict = {}
    _schema_entries_dict = {}
    _index_entries_dict = {}

    def __init__(self) -> None:
        self._table_name = self.__class__.meta["table_name"][0]
        self._headers = self.__class__.get_headers()
        self._required_headers = self.__class__.get_required_headers()
        self._headers_type = self.__class__.get_headers_type()
        self._headers_default = self.__class__.get_headers_default()
        self._field_dict = self.__class__.get_field_dict()
        self._field_conditions = self.__class__.get_field_conditions()
        self._field_index_dict = self.__class__.get_field_index_dict()

        for k, v in self._headers_default.items():
            if not k.startswith("_"):
                setattr(self, k, v)
    
    def to_table_format(self) -> dict:
        """
        This is the method to form the object to the table data.

        Returns:
            ret_dict (dict): The table data.
        """
        ret_dict = {}
        _headers = self._headers
        for _header in _headers:
            if _header in self.__dict__:
                ret_dict[_header] = self.__dict__[_header]
        return ret_dict

    @classmethod
    def from_table_format(cls, table_data: list) -> list:
        """
        This is the method to form the table data to the object.

        Args:
            table_data (list): The table data.
        
        Returns:
            ret_list (list): The list of object.
        """
        ret_list = []
        fields = cls.get_field_dict()
        for row in table_data:
            new_obj = cls()
            for key in row:
                if key in fields:
                    setattr(new_obj, key, row[key])
        return ret_list

    @classmethod
    def _meta_inspector(cls):
        def lack_inspector(required_list):
            lack_list = []
            for key in required_list:
                if key not in cls.meta:
                    lack_list.append(key)
            if len(lack_list) > 0:
                raise Exception(f"( {cls.__name__} ) Lack of meta: {', '.join(lack_list)}")

        if not hasattr(cls, "meta"):
            raise Exception(f"( {cls.__name__} ) No meta")
        
        lack_inspector(cls.META_REQUIRED)

        if cls.meta["init_type"] == cls.TABLE:
            lack_inspector(cls.TABLE_META_REQUIRED)
            if type(cls.meta["table_name"]) != list:
                raise Exception(f"( {cls.__name__} ) table_name should be list")
            elif cls.meta["init_type"] == cls.TABLE and len(cls.meta["table_name"]) == 0:
                raise Exception(f"( {cls.__name__} ) table_name should not be empty if init_type is table")
            elif cls.meta["init_type"] == cls.TABLE and len(cls.meta["table_name"]) > 1:
                raise Exception(f"( {cls.__name__} ) table_name should not be more than one")
        
        elif cls.meta["init_type"] == cls.SCHEMA:
            lack_inspector(cls.SCHEMA_META_REQUIRED)
        
        else:
            raise Exception(f"( {cls.__name__} ) Invalid init_type {cls.meta['init_type']}")

        if type(cls.meta["schema_name"]) != list:
            raise Exception(f"( {cls.__name__} ) schema_name should be list")
        elif len(cls.meta["schema_name"]) == 0:
            raise Exception(f"( {cls.__name__} ) schema_name should not be empty")
        elif cls.meta["init_type"] == cls.SCHEMA and len(cls.meta["schema_name"]) > 1:
            raise Exception(f"( {cls.__name__} ) schema_name should not be more than one if init_type is schema")

    @classmethod
    def _conditional_init_inspector(cls):
        if cls.meta["conditional_init"]:
            lack_list = []
            for key in cls.CONDITIONAL_INIT_REQUIRED:
                if key not in cls.__dict__:
                    lack_list.append(key)
            if len(lack_list) > 0:
                raise Exception(f"( {cls.__name__} ) Lack of conditional init method: {', '.join(lack_list)}")
        else:
            raise Exception(f"( {cls.__name__} ) conditional_init should be True")

    @classmethod
    def get_field_dict(cls) -> dict:
        """
        This is the method to get the field dictionary.

        Returns:
            field_dict (dict): The field dictionary.
        """
        return {k: v for k, v in cls.__dict__.items() if isinstance(v, PostgresField)}
    
    @classmethod
    def get_field_conditions(cls) -> str:
        """
        This is the method to get the field conditions.

        Returns:
            sql conditions (string): The field conditions.
        """
        return ', '.join(f"{k} {v}" for k, v in cls.__dict__.items() if isinstance(v, PostgresField))

    @classmethod
    def get_field_index_dict(cls) -> dict:
        """
        This is the method to get the index type of the headers.

        Returns:
            index_dict (dict): The index type of the headers.
        """
        return {k: v.index_type for k, v in cls.__dict__.items() if isinstance(v, PostgresField) and v.index_type != ""}

    @classmethod
    def get_headers(cls) -> list:
        """
        This is the method to get the headers.

        Returns:
            headers (list): The headers.
        """
        return [k for k, v in cls.__dict__.items() if isinstance(v, PostgresField)]

    @classmethod
    def get_required_headers(cls) -> list:
        """
        This is the method to get the required headers.

        Returns:
            required_headers (list): The required headers.
        """
        return [k for k, v in cls.__dict__.items() if isinstance(v, PostgresField) and v.required]

    @classmethod
    def get_headers_type(cls) -> dict:
        """
        This is the method to get the headers type.

        Returns:
            dic (dict): The headers type.
        """
        return {k: v.field_type for k, v in cls.__dict__.items() if isinstance(v, PostgresField)}

    @classmethod
    def get_headers_default(cls) -> dict:
        """
        This is the method to get the default value of the headers.

        Returns:
            dic (dict): The default value of a row in the table.

        Limitation:
            \- If the default value is "''", it will be changed to "".
            \- If the default value is "ARRAY[]::", it will be changed to [].
            \- If the field is serial and primary key, it will be ignored.
        """
        dic = {}
        for k, v in cls.__dict__.items():
            if isinstance(v, PostgresField):
                if type(v.default) == str and v.default.startswith("ARRAY[]::"):
                    dic[k] = []
                elif v.serial and v.primary_key:
                    pass
                elif v.default == "''":
                    dic[k] = ""
                else:
                    dic[k] = v.default
        return dic
    
    @classmethod
    def form_schema_sql(cls, inspect=True) -> str:
        if inspect:
            cls._meta_inspector()
        if cls.meta["init_type"] != cls.SCHEMA:
            raise Exception(f"( {cls.__name__} ) Invalid init_type ( {cls.meta['init_type']} )")
        return f"""
        CREATE SCHEMA IF NOT EXISTS {cls.meta["schema_name"][0]} AUTHORIZATION {cls.meta["authorization"]};
        """

    @classmethod
    def form_table_sql(cls, inspect=True) -> str:
        """
        This is the method to form the table sql command.

        Args:
            inspect (boolean, optional): Defaults to True. If True, the meta data will be inspected.
        
        Returns:
            table_sql (string): The table sql command.
        """
        if inspect:
            cls._meta_inspector()
        if cls.meta["init_type"] != cls.TABLE:
            raise Exception(f"( {cls.__name__} ) Invalid init_type ( {cls.meta['init_type']} )")
        table_sql = ""
        for item in cls.meta["schema_name"]:
            table_sql += f"""
            CREATE TABLE IF NOT EXISTS {item}.{cls.meta["table_name"][0]} ( {cls.get_field_conditions()} );
            """
        return table_sql

    @classmethod
    def form_table_conditional_sql(cls, inspect=True) -> str:
        """
        This is the method to form the table with conditional initialization sql command.
        
        Args:
            inspect (boolean, optional): Defaults to True. If True, the meta data and conditional init method will be inspected.
        
        Returns:
            table_sql (string): The table sql command.
        """
        if inspect:
            cls._meta_inspector()
            cls._conditional_init_inspector()
        if cls.meta["init_type"] != cls.TABLE:
            raise Exception(f"( {cls.__name__} ) Invalid init_type ( {cls.meta['init_type']} )")
        table_sql = ""
        for item in cls.meta["schema_name"]:
            if_conditions = cls.if_initialize(schema_name=item, table_name=cls.meta["table_name"][0])
            else_conditions = cls.else_initialize(schema_name=item, table_name=cls.meta["table_name"][0])
            table_sql += f"""
            DO $$
            BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{item}' AND table_name = '{cls.meta["table_name"][0]}') THEN
                CREATE TABLE {item}.{cls.meta["table_name"][0]} ( {cls.get_field_conditions()} );
                {if_conditions}
            {"ELSE " + else_conditions if else_conditions else ""}
            END IF;
            END$$;
            """
        return table_sql

    @classmethod
    def form_index_sql(cls, inspect=True) -> str:
        """
        This is the method to form the index sql command.

        Args:
            inspect (boolean, optional): Defaults to True. If True, the meta data will be inspected.
        
        Returns:
            index_sql (string): The index sql command.
        """
        if inspect:
            cls._meta_inspector()
        if cls.meta["init_type"] != cls.TABLE:
            raise Exception(f"( {cls.__name__} ) Invalid init_type ( {cls.meta['init_type']} )")
        index_sql = ""
        index_dict = cls.get_field_index_dict()
        for schema in cls.meta["schema_name"]:
            for column, index_method in index_dict.items():
                index_sql += f"""
                CREATE INDEX IF NOT EXISTS idx_{column} ON {schema}.{cls.meta["table_name"][0]} USING {index_method.upper()} ({column});
                """
        return index_sql

    @classmethod
    def create_sql(cls):
        """
        This is the method to create all the sql command, including schema, table, index.
        """
        cls._meta_inspector()

        if cls.meta["init_type"] == cls.TABLE:
            if cls.meta["conditional_init"]:
                cls._conditional_init_inspector()
                cls._table_entries_dict[cls.__name__] = cls.form_table_conditional_sql(inspect=False)
            else:
                cls._table_entries_dict[cls.__name__] = cls.form_table_sql(inspect=False)
            cls._index_entries_dict[cls.__name__] = cls.form_index_sql(inspect=False)
        elif cls.meta["init_type"]== cls.SCHEMA:
            cls._schema_entries_dict[cls.__name__] = cls.form_schema_sql(inspect=False)
        else:
            raise Exception("Invalid meta type")

    @classmethod
    def execute_sql(cls, postgres_handler: PostgresHandler, sql_cb=None):
        """
        This is the method to execute the sql command.

        Args:
            postgres_handler (PostgresHandler): The postgres handler to execute the sql command.
            sql_cb (function, optional): The callback function to get the sql command. Defaults to None.
                If sql_cb is None, the sql command will be formed by the entries in the class.
                Example:
                    TableObj.execute_sql(DH, TableObj.form_table_sql)
        """
        if sql_cb:
            sql_input = sql_cb()
        else:
            sql_input = f"{' '.join(cls._schema_entries_dict.values())} {' '.join(cls._table_entries_dict.values())} {' '.join(cls._index_entries_dict.values())}"
        postgres_handler._execute_sql(db_operation_mode.MODE_DB_NORMAL, sql_input)

    @classmethod
    def clear_sql(cls):
        """
        This is the method to clear the sql command.
        """
        cls._table_entries_dict = {}
        cls._schema_entries_dict = {}
        cls._index_entries_dict = {}

    @classmethod
    def if_initialize(cls, schema_name, table_name):
        """
        ** customized method **
        if_initialize is the method to form the conditional initialization sql command.
        If the table does not exist, the if_initialize will be executed.

        Args:
            schema_name (string): The schema name.
            table_name (string): The table name.

        Example:
            return f"INSERT INTO {schema_name}.{table_name} (camera_ip, roi_id) VALUES ('10.0.0.123', 1);"
        """
        pass

    @classmethod
    def else_initialize(cls, schema_name, table_name):
        """
        ** customized method **
        else_initialize is the method to form the conditional initialization sql command.
        If the table already exists, the else_initialize will be executed.ｆ

        Args:
            schema_name (string): The schema name.
            table_name (string): The table name.

        Example:
            return f"UPDATE {schema_name}.{table_name} SET camera_ip='10.0.0.124' WHERE roi_id=1;"
        """
        pass

class PostgresField:
    """
    This is the class to define the postgres field.

    Args:
        field_type (string, optional): Defaults to "".
        required (boolean, optional): Defaults to False.
        default (string, optional): Defaults to None.
        serial (boolean, optional): Defaults to False.
        primary_key (boolean, optional): Defaults to False.
        unique (boolean, optional): Defaults to False.
        check (string, optional): Defaults to "".
        generated_as (string, optional): Defaults to "".
        index_type (string, optional): Defaults to "". (e.g. BTREE, HASH, GIST, GIN, BRIN, SPGIST)
    """
    def __init__(self, field_type="", required=False, default=None, serial=False, primary_key=False,
                       unique=False, check="", generated_as="", index_type=""):
        self.field_type = field_type
        self.default = default
        self.required = required
        self.serial = serial
        self.primary_key = primary_key
        self.unique = unique
        self.check = check
        self.generated_as = generated_as
        self.index_type = index_type
    
    def __str__(self):
        return f"{self.field_type.upper()}{self.get_default()}{self.get_required()}{self.get_serial()}{self.get_primary_key()}{self.get_unique()}{self.get_check()}{self.get_generated_as()}"

    def get_default(self):
        return f" DEFAULT {self.default}" if self.default is not None else ""
    
    def get_required(self):
        return " NOT NULL" if self.required else ""
    
    def get_serial(self):
        return " SERIAL" if self.serial else ""
    
    def get_primary_key(self):
        return " PRIMARY KEY" if self.primary_key else ""
    
    def get_unique(self):
        return " UNIQUE" if self.unique else ""

    def get_check(self):
        return f" CHECK ({self.check})" if self.check else ""

    def get_generated_as(self):
        return f" GENERATED ALWAYS AS ({self.generated_as}) STORED" if self.generated_as else ""

# if __name__ == "__main__":
#     ph = PostgresHandler("10.0.0.32", 5432, "postgres", "postgres", "1234") # should print "Connection with database is OK"
#     r = None
#     r = ph._execute_sql(db_operation_mode.MODE_DB_NORMAL, "INSERT INTO lot_table (lot_id, camera_ip, roi_id) VALUES (1,'192.168.1.179', 5);")
#     r = ph._execute_sql(db_operation_mode.MODE_DB_W_ARGS, "INSERT INTO login_table (username, lastupdatetime) VALUES (%s, %s);", ["linga", None])
#     r = ph._execute_sql(db_operation_mode.MODE_DB_W_RETURN_WO_ARGS, "select * from information_schema.columns where table_name = 'login_table';")
#     r = ph._execute_sql(db_operation_mode.MODE_DB_W_RETURN_AND_ARGS, "SELECT camera_ip, roi_id FROM unique_test WHERE camera_ip LIKE%s;", ["192.168%"])
#     r = ph._execute_sql(db_operation_mode.MODE_DB_W_ARGS, "INSERT INTO unique_test (camera_ip, roi_id) VALUES (%s, %s);", [["192.168.1.80", 4], ["192.168.1.80", 3]], multiple=True)
    
#     print(ph.get_table_list())
#     print(ph.get_headers("order_table"))
#     r = ph._execute_sql(db_operation_mode.MODE_DB_W_ARGS, "UPDATE inventory_table SET material_id=%s, material_name=%s WHERE material_id=%s", ["14", "黃豆", "14"])

#     r = ph.get_data("vpgs.lot_table")
#     r = ph.get_data("vpgs.camera_setting_table")
#     r = ph.get_data("vpgs.lot_table", 
#                     target_column_list=["roi_id", "camera_ip"], 
#                     conditional_rule_list=[("lot_id=", 1)],
#                     order_by_list=["roi_id DESC"],
#                     limit_number=20
#                     )
#     r = ph.update_data("inventory_table", [{"material_id":"14", "material_name":"粉粿", "store_id":"DDA"}, {"material_id":"13", "material_name":"芋泥", "store_id":"DDA"}], ["material_id", "store_id"])
#     r = ph.add_data("login_table", [{"username":"linga", "lastupdatetime":123456}, {"username":"linga", "lastupdatetime":456789}], ["lastupdatetime", "username"])
#     r = ph.add_data("vpgs.lot_table", [{"lot_id":1,"camera_ip":"123.123","roi_id":2}],["lot_id","camera_ip","roi_id"])
#     r = ph.add_data("vpgs.lot_table", [{"lot_id":2,"camera_ip":"123.123","roi_id":3}],["lot_id","camera_ip","roi_id"])
#     r = ph.add_data("vpgs.camera_setting_table", [{"camera_ip":"10.0.7.105","camera_type":"his_vpgs","camera_port":554,"roi_id":2,
#                                                        "roi_data":json.dumps({"x": 640, "y": 680, "w": 640, "h": 360}),
#                                                        "motion_level":100,"recognition_server_ip": "127.0.0.1"
#                                                        }],["camera_ip","camera_type","camera_port","roi_id","roi_data","motion_level","recognition_server_ip"])
    

#     r = ph.update_data("vpgs.lot_table", [{"lot_id":1,"camera_ip":"127.0.0.1","roi_id":100}],["lot_id"])
#     r = ph.delete_data("login_table", [{"username":"linga", "lastupdatetime":123456, "test": 1234569789}], ["username"])
#     r = ph.delete_data("vpgs.lot_table",[{"camera_ip":"127.0.0.1","lot_id":3}],["lot_id"])
#     print(r)