import mongoengine as mongo
from pymongo.errors import ServerSelectionTimeoutError
from pymongo import MongoClient
import logging
import datetime
from bson import ObjectId

class DocumentHandler(mongo.Document):
    meta = { "abstract": True}

    @classmethod
    def format_data(cls, mongo_obj):
        sub_item = mongo_obj.to_mongo().to_dict()
        return sub_item

    @classmethod
    def format_data_list(cls, mongo_obj_list):
        ret = []
        for item in mongo_obj_list:
            sub_item = item.to_mongo().to_dict()
            ret.append(sub_item)
        return ret
    
    @classmethod
    def format_and_validate_document(cls, data):
        ret = []
        for item in data:
            add_item = cls(**item)
            add_item.validate()
            ret.append(add_item)
        return ret
    
    @classmethod
    def _process_id_condition(cls, conditions: dict) -> dict:
        if conditions.get("_id"):
            id_condition = conditions["_id"]
            if isinstance(id_condition, str):
                # {_id: "123456"} -> {_id: ObjectId("123456")}
                id_condition = ObjectId(id_condition)
            elif isinstance(id_condition, dict):
                for key, value in id_condition.items():
                    if isinstance(value, str):
                        # {_id: {"$eq": "123456"}} -> {_id: {"$eq": ObjectId("123456")}}
                        id_condition[key] = ObjectId(value)
                    elif isinstance(value, list):
                        # {_id: {"$in": ["123456", "789012"]}} -> {_id: {"$in": [ObjectId("123456"), ObjectId("789012")]
                        id_condition[key] = [ObjectId(v) if isinstance(v, str) else v for v in value]
                    elif isinstance(value, dict):
                        # {_id: {"$not": {"$eq": "123456"}}} -> {_id: {"$not": {"$eq": ObjectId("123456")}}
                        id_condition[key] = cls._process_id_condition(value)
            conditions["_id"] = id_condition
        else:
            for key, value in conditions.items():
                if key.startswith("$"):
                    if isinstance(value, str):
                        # {"#eq": "123456} -> {"#eq": ObjectId("123456")}
                        conditions[key] = ObjectId(value)
                    elif isinstance(value, list):
                        # {"$or": [{"_id": "123456"}, {"_id": "789012"}]} -> {"$or": [{"_id": ObjectId("123456")}, {"_id": ObjectId("789012")}]
                        for item in value:
                            # {"_id": "123456"} -> {"_id": ObjectId("123456")}
                            item = cls._process_id_condition(item)
        return conditions

    @classmethod
    def handle_modified_time(cls, data: list, time_format="%Y%m%d%H%M%S%f") -> list:
        for item in data:
            item["modified_time"] = datetime.datetime.now().strftime(time_format)[:-3]
        return data

    @classmethod
    def get_headers(cls):
        return list(cls._fields_ordered)

    @classmethod
    def add_data(cls, data):
        add_data = cls.format_and_validate_document(data)
        ret = cls.objects.insert(add_data)
        return cls.format_data_list(ret)

    @classmethod
    def get_data(cls, conditions, order_by_list=[]):
        if conditions:
            conditions = cls._process_id_condition(conditions)
            raw_data = cls.objects(__raw__=conditions)
        else:
            raw_data = cls.objects
        return cls.format_data_list(raw_data.order_by(*order_by_list))
    
    @classmethod
    def update_data(cls, conditions, update_data):
        if conditions:
            conditions = cls._process_id_condition(conditions)
            raw_data = cls.objects(__raw__=conditions)
        else:
            raw_data = cls.objects
        return cls.format_data(raw_data.modify(__raw__={"$set": update_data}, new=True))
    
    @classmethod
    def delete_data(cls, conditions):
        if conditions:
            conditions = cls._process_id_condition(conditions)
            raw_data = cls.objects(__raw__=conditions)
        else:
            raw_data = cls.objects
        
        return raw_data.delete()

class MongoHandler:
    """
    MongoDB Handler

    Args:
        alias (str): alias of MongoDB
        host (str): host of MongoDB
        port (int): port of MongoDB
        database (str): database of MongoDB
        username (str): username of MongoDB
        password (str): password of MongoDB
        schemas (dict[str, DocumentHandler]): {
            schema_name: schema
        }
    """
    def __init__(self, alias: str, host: str, port: int, database: str, username: str, password: str, schemas: dict[str, DocumentHandler], **kwargs):
        self.db_type = "mongo"
        self.alias = alias
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.timeout = kwargs.get("timeout") if kwargs.get("timeout") else 5000

        if not logging.getLogger().hasHandlers():
            logging.basicConfig(level=logging.INFO)
        
        self._create_schemas(schemas)
        self.connect(first_time=True)

    def connect(self, first_time=False) -> None:
        """
        This function is used to connect to MongoDB

        Args:
            first_time (bool, optional): connect to MongoDB for the first time. Defaults to False.
        """
        try:
            if first_time:
                client = MongoClient(host=self.host, port=self.port, username=self.username, password=self.password, serverSelectionTimeoutMS=self.timeout)
                client.server_info()
                logging.info(" [MongoHandler] Connect to MongoDB successfully ")
                client.close()
            else:
                mongo.connect(alias=self.alias, db=self.database, host=self.host, port=self.port, username=self.username, password=self.password, serverSelectionTimeoutMS=self.timeout)
        except ServerSelectionTimeoutError as e:
            logging.warning(" [MongoHandler] Connect to MongoDB failed: \n{} ".format(e))
        except Exception as e:
            logging.warning(" [MongoHandler] Connect to MongoDB failed: \n{} ".format(e))

    def disconnect(self) -> None:
        """
        This function is used to disconnect from MongoDB
        """
        mongo.disconnect(alias=self.alias)
    
    def _create_schemas(self, schemas: dict[str, DocumentHandler]) -> None:
        """
        Create schemas

        Args:
            schemas (dict): {
                schema_name: schema
            }
        """
        for schema_name, schema in schemas.items():
            self.__dict__[schema_name] = schema
    
    def get_headers(self, schema_name: str) -> dict:
        """
        Get headers of a schema
        
        Args:
            schema_name (str): name of schema
        
        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list[dict]
            }
        """
        self.connect()
        try:
            data = self.__dict__[schema_name].get_headers()
            indicator = True
            message = "Get headers from MongoDB successfully"
        except Exception as e:
            data = []
            indicator = False
            message = "[MongoHandler] Get headers from MongoDB failed: \n{}".format(e)
            logging.warning(message)
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": data}

    def get_data(self, schema_name: str, conditions={}, order_by_list=[]) -> dict:
        """
        Get data from MongoDB
        
        Args:
            schema_name (str): name of schema (collection name in MongoDB)
            conditions (dict): conditions of query (follow MongoDB query syntax)
            order_by_list (list, optional): order by. + for ascending and - for descending. e.g. ["-modified_time"]. Defaults to [].
            
        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list[dict]
            }
        
        Operators(use "$" to represent the operator in conditions):
            - comparison operators:
                $eq: match values that are equal to a specified value (e.g. {"name": {"$eq": "John"}})
                $gt: match values that are greater than a specified value (e.g. {"age": {"$gt": 18}})
                $gte: match values that are greater than or equal to a specified value (e.g. {"age": {"$gte": 18}})
                $in: match any of the values specified in an array (e.g. {"name": {"$in": ["John", "Peter"]}})
                $lt: match values that are less than a specified value (e.g. {"age": {"$lt": 18}})
                $lte: match values that are less than or equal to a specified value (e.g. {"age": {"$lte": 18}})
                $ne: match all values that are not equal to a specified value (e.g. {"age": {"$ne": 18}})
                $nin: match none of the values specified in an array (e.g. {"name": {"$nin": ["John", "Peter"]}})
            - logical operators:
                $or: match any of the specified conditions (e.g. {"$or": [{"name": "John"}, {"name": "Peter"}]})
                $and: match all of the specified conditions (e.g. {"$and": [{"name": "John"}, {"age": 18}]})
                $not: invert the effect of a query expression (e.g. {"name": {"$not": {"$eq": "John"}}})
                $nor: match none of the specified conditions (e.g. {"$nor": [{"name": "John"}, {"name": "Peter"}]})
            - element operators:
                $exists: match documents that have the specified field (e.g. {"name": {"$exists": True}})
                $type: match documents that have the specified type (e.g. {"age": {"$type": "int"}})
            - evaluation operators:
                $mod: match documents where the field value equals the specified modulus (e.g. {"age": {"$mod": [5, 0]}})
                $regex: match documents that satisfy a JavaScript expression (e.g. {"name": {"$regex": "^J"}})
                $text: perform text search (e.g. {"$text": {"$search": "John"}})
                $where: match documents that satisfy a JavaScript expression (e.g. {"$where": "this.name === 'John'"})
            - array operators:
                $elemMatch: match documents that contain an array field with at least one element that matches all the specified query criteria (e.g. {"favorite": {"$elemMatch": {"title": "react"}}})
                $size: match documents that have an array field with the specified number of elements (e.g. {"favorite": {"$size": 2}})
                $all: match arrays that contain all elements specified in the query (e.g. {"favorite": {"$all": [{"title": "react"}, {"author": "open_source_guru"}]}})
            - comment operators:
                $comment: add a comment to a query predicate (e.g. {"name": {"$eq": "John", "$comment": "This is a comment"}})
                $meta: control the metadata returned in a text search (e.g. {"$text": {"$search": "John", "$meta": "textScore"}})
            - projection operators:
                $slice: limit the number of elements in an array that a $push operation inserts (e.g. {"$push": {"favorite": {"$each": [{"title": "react"}, {"title": "linux"}], "$slice": 2}}})
                $sort: sort elements in an array during a $push operation (e.g. {"$push": {"favorite": {"$each": [{"title": "react"}, {"title": "linux"}], "$sort": {"title": 1}}}})
        TIPS:
            $or can be replaced by $in, e.g. {"$or": [{"name": "John"}, {"name": "Peter"}]} can be replaced by {"name": {"$in": ["John", "Peter"]}}
            $and can be replaced by $all, e.g. {"$and": [{"name": "John"}, {"age": 18}]} can be replaced by {"$all": [{"name": "John"}, {"age": 18}]}
            $not + $eq can be replaced by $ne, e.g. {"name": {"$not": {"$eq": "John"}}} can be replaced by {"name": {"$ne": "John"}}
            $nor can be replaced by $nin, e.g. {"$nor": [{"name": "John"}, {"name": "Peter"}]} can be replaced by {"name": {"$nin": ["John", "Peter"]}}
        """
        self.connect()

        try:
            schema: DocumentHandler = self.__dict__[schema_name]

            indicator = True
            message = "Get data from MongoDB successfully"
            data = schema.get_data(conditions, order_by_list)
        except Exception as e:
            indicator = False
            message = "[MongoHandler] Get data from MongoDB failed: {}".format(e)
            data = []
            logging.warning(message)
        
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": data}
    
    def add_data(self, schema_name: str, input_data: list[dict]) -> dict:
        """
        Add data to MongoDB and return the data added
        
        Args:
            schema_name (str): name of schema
            input_data (list[dict]): data to add

        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list[dict]
            }
        """
        self.connect()

        try:
            schema: DocumentHandler = self.__dict__[schema_name]

            indicator = True
            message = "Add data to MongoDB successfully"
            data = schema.add_data(input_data)
        except Exception as e:
            indicator = False
            message = "[MongoHandler] Add data to MongoDB failed: {}".format(e)
            data = []
            logging.warning(message)
        
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": data}
    
    def update_data(self, schema_name: str, conditions: dict, update_data: dict) -> dict:
        """
        Update data in MongoDB and return the data updated
        
        Args:
            schema_name (str): name of schema
            conditions (dict): conditions of query (follow MongoDB query syntax)
            update_data (dict): data to update
        
        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list
            }
        """
        self.connect()

        try:
            schema: DocumentHandler = self.__dict__[schema_name]

            indicator = True
            message = "Update data in MongoDB successfully"
            data = schema.update_data(conditions, update_data)
        except Exception as e:
            indicator = False
            message = "[MongoHandler] Update data in MongoDB failed: {}".format(e)
            data = []
            logging.warning(message)

        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": data}
    
    def delete_data(self, schema_name: str, conditions: dict) -> dict:
        """
        Delete data from MongoDB
        
        Args:
            schema_name (str): name of schema
            conditions (dict): conditions of query (follow MongoDB query syntax)

        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list
            }
        """
        self.connect()
        
        try:
            schema: DocumentHandler = self.__dict__[schema_name]

            indicator = True
            message = "Delete data from MongoDB successfully"
            delete_count = schema.delete_data(conditions)

            data = [{"deleted_count": delete_count, "conditions": conditions}]
        except Exception as e:
            indicator = False
            message = "[MongoHandler] Delete data from MongoDB failed: {}".format(e)
            data = []
            logging.warning(message)
        
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": data}

class MongoWidget:
    """
    MongoDB Widget, this class is used to connect to MongoDB and get data from MongoDB.
    If you want to add data to MongoDB, please use MongoHandler class for strict data validation.

    Args:
        host (str): host of MongoDB
        port (int): port of MongoDB
        database (str): database of MongoDB
        username (str): username of MongoDB
        password (str): password of MongoDB
    """
    _valid_ret_type_list = ["empty", "jsonable", "original"]

    def __init__(self, host: str, port: int, database: str, username: str, password: str) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

        self.timeout = 5000
        self.client = None

        if not logging.getLogger().hasHandlers():
            logging.basicConfig(level=logging.INFO)
        
        self.connect(first=True)

    def connect(self, first=False):
        """
        Connect to MongoDB
        
        Args:
            first (bool, optional): connect to MongoDB for the first time. Defaults to False.
        """
        try:
            self.client = MongoClient(host=self.host, port=self.port, username=self.username, password=self.password, serverSelectionTimeoutMS=self.timeout)
            if first:
                self.client.server_info()
                logging.info(" [MongoHandler] Connect to MongoDB successfully ")
                self.disconnect()
        except ServerSelectionTimeoutError as e:
            logging.warning(" [MongoHandler] Connect to MongoDB failed: \n{} ".format(e))
        except Exception as e:
            logging.warning(" [MongoHandler] Connect to MongoDB failed: \n{} ".format(e))
    
    def disconnect(self):
        """
        Disconnect from MongoDB
        """
        self.client.close()
    
    def form_jsonable_data(self, data_dict: dict) -> dict:
        for key, value in data_dict.items():
            if isinstance(value, ObjectId):
                data_dict[key] = str(value)
            elif isinstance(value, datetime.datetime):
                data_dict[key] = value.timestamp()
        return data_dict

    def _process_id_condition(self, conditions: dict) -> dict:
        if conditions.get("_id", None):
            id_condition = conditions["_id"]
            if isinstance(id_condition, str):
                # {_id: "123456"} -> {_id: ObjectId("123456")}
                id_condition = ObjectId(id_condition)
            elif isinstance(id_condition, dict):
                for key, value in id_condition.items():
                    if isinstance(value, str):
                        # {_id: {"$eq": "123456"}} -> {_id: {"$eq": ObjectId("123456")}}
                        id_condition[key] = ObjectId(value)
                    elif isinstance(value, list):
                        # {_id: {"$in": ["123456", "789012"]}} -> {_id: {"$in": [ObjectId("123456"), ObjectId("789012")]
                        # id_condition[key] = [ObjectId(v) for v in value]
                        id_condition[key] = [ObjectId(v) if isinstance(v, str) else v for v in value]
                    elif isinstance(value, dict):
                        # {_id: {"$not": {"$eq": "123456"}}} -> {_id: {"$not": {"$eq": ObjectId("123456")}}
                        id_condition[key] = self._process_id_condition(value)
            conditions["_id"] = id_condition
        else:
            for key, value in conditions.items():
                if key.startswith("$"):
                    if isinstance(value, str):
                        # {_id: {"$not": {"$eq": "123456"}}} -> {_id: {"$not": {"$eq": ObjectId("123456")}}
                        #     |
                        #     v
                        # {"#eq": "123456} -> {"#eq": ObjectId("123456")}
                        conditions[key] = ObjectId(value)
                    elif isinstance(value, list):
                        # {"$or": [{"_id": "123456"}, {"_id": "789012"}]} -> {"$or": [{"_id": ObjectId("123456")}, {"_id": ObjectId("789012")}]
                        for item in value:
                            # {"_id": "123456"} -> {"_id": ObjectId("123456")}
                            item = self._process_id_condition(item)
        return conditions

    def get_data(self, collection: str, conditions={}, order_by_list=[], ret_type="original") -> dict:
        """
        Get data from MongoDB
        
        Args:
            collection (str): name of collection
            conditions (dict): conditions of query (follow MongoDB query syntax)
            order_by_list (list of tuple): order by. Elements of tuple are (key, order), 
                                           order is 1 for ascending and -1 for descending.
                                           e.g. [("modified_time", -1)]
            ret_type (str, optional): return type. Defaults to "original". Should be one of ["jsonable", "original"].

        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list[dict]
            }
        """
        self.connect()
        try:
            indicator = True
            message = "Get data from MongoDB successfully"
            if ret_type not in self.__class__._valid_ret_type_list:
                raise ValueError("Invalid ret_type: {}".format(ret_type))
            elif ret_type == "empty":
                raise ValueError("ret_type cannot be 'empty' for get_data() function.")
            
            conditions = self._process_id_condition(conditions)
            
            if order_by_list:
                data = list(self.client[self.database][collection].find(conditions).sort(order_by_list))
            else:
                data = list(self.client[self.database][collection].find(conditions))

            if ret_type == "jsonable":
                for item in data:
                    item = self.form_jsonable_data(item)
        except Exception as e:
            indicator = False
            message = "[MongoWidget] Get data from MongoDB failed: {}".format(e)
            data = []
            logging.warning(message)
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": data}
    
    def _add_data(self, collection: str, data: list[dict], ret_type="original") -> dict:
        """
        Add data to MongoDB
        
        Args:
            collection (str): name of collection
            data (list[dict]): data to add
            ret_type (str, optional): return type. Defaults to "original". Should be one of ["empty", "jsonable", "original"].

        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list[dict]
            }
        """
        self.connect()

        try:
            indicator = True
            message = "Add data to MongoDB successfully"
            if ret_type not in self.__class__._valid_ret_type_list:
                raise ValueError("Invalid ret_type: {}".format(ret_type))
            
            ret = self.client[self.database][collection].insert_many(data)

            if ret_type == "empty":
                ret = []
            else:
                condition_params = list(ret.inserted_ids)
                ret = list(self.client[self.database][collection].find({"_id": {"$in": condition_params}}))
                if ret_type == "jsonable":
                    for item in ret:
                        item = self.form_jsonable_data(item)
        except Exception as e:
            indicator = False
            message = "[MongoWidget] Add data to MongoDB failed: {}".format(e)
            ret = []
            logging.warning(message)
        
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": ret}

    def _update_data(self, collection: str, conditions: dict, update_data: dict, ret_type="original") -> dict:
        """
        Update data in MongoDB
        
        Args:
            collection (str): name of collection
            conditions (dict): conditions of query (follow MongoDB query syntax)
            update_data (dict): data to update
            ret_type (str, optional): return type. Defaults to "original". Should be one of ["empty", "jsonable", "original"].

        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list
            }
        """
        self.connect()

        try:
            indicator = True
            message = "Update data in MongoDB successfully"
            if ret_type not in self.__class__._valid_ret_type_list:
                raise ValueError("Invalid ret_type: {}".format(ret_type))
            
            ret = self.client[self.database][collection].update_many(conditions, {"$set": update_data})
            if ret_type == "empty":
                ret = []
            else:
                ret = list(self.client[self.database][collection].find(conditions))
                if ret_type == "jsonable":
                    for item in ret:
                        item = self.form_jsonable_data(item)
        except Exception as e:
            indicator = False
            message = "[MongoWidget] Update data in MongoDB failed: {}".format(e)
            ret = []
            logging.warning(message)
        
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": ret}

    def _delete_data(self, collection: str, conditions: dict):
        """
        Delete data from MongoDB
        
        Args:
            collection (str): name of collection
            conditions (dict): conditions of query (follow MongoDB query syntax)

        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list
            }
        """
        self.connect()

        try:
            indicator = True
            message = "Delete data from MongoDB successfully"
            ret = [{"deleted_count": self.client[self.database][collection].delete_many(conditions).deleted_count, "conditions": conditions}]
        except Exception as e:
            indicator = False
            message = "[MongoWidget] Delete data from MongoDB failed: {}".format(e)
            ret = []
            logging.warning(message)
        
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": ret}
