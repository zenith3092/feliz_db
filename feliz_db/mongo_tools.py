import mongoengine as mongo
from pymongo.errors import ServerSelectionTimeoutError
from pymongo import MongoClient
import logging
import datetime
from bson import ObjectId

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
        schemas (dict): {
            "schema_name": schema
        }
    """
    def __init__(self, alias: str, host: str, port: int, database: str, username: str, password: str, schemas: str):
        self.db_type = "mongo"
        self.alias = alias
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.timeout = 5000 # ms

        if not logging.getLogger().hasHandlers():
            logging.basicConfig(level=logging.INFO)
        
        self.create_schemas(schemas)
        self.connect(first_time=True)

    def connect(self, first_time=False) -> None:
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
        mongo.disconnect(alias=self.alias)
    
    def create_schemas(self, schemas: dict) -> None:
        """
        Create schemas

        Args:
            schemas (dict): {
                "schema_name": schema
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
                "data": list
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
        return {"indicator": indicator, "message": message, "data": data}

    def get_data(self, schema_name: str, conditions={}, order_by_list=[], ret_type="jsonable") -> dict:
        """
        Get data from MongoDB
        
        Args:
            schema_name (str): name of schema (collection name in MongoDB)
            conditions (dict): conditions of query (follow MongoDB query syntax)
            order_by (list, optional): order by. + for ascending and - for descending. e.g. ["-modified_time"]. Defaults to [].
            ret_type (str, optional): return type. Defaults to "jsonable". Should be one of ["jsonable", "original"].
            
        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "formatted_data": list
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
            $not can be replaced by $ne, e.g. {"name": {"$not": {"$eq": "John"}}} can be replaced by {"name": {"$ne": "John"}}
            $nor can be replaced by $nin, e.g. {"$nor": [{"name": "John"}, {"name": "Peter"}]} can be replaced by {"name": {"$nin": ["John", "Peter"]}}
        """
        self.connect()
        try:
            indicator = True
            message = "Get data from MongoDB successfully"
            data = self.__dict__[schema_name].get_data(conditions, order_by_list, ret_type)
        except Exception as e:
            indicator = False
            message = "[MongoHandler] Get data from MongoDB failed: {}".format(e)
            data = []
            logging.warning(message)
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": data}
    
    def add_data(self, schema_name: str, input_data: list, ret_type="jsonable") -> dict:
        """
        Add data to MongoDB and return the data added
        
        Args:
            schema_name (str): name of schema
            input_data (list of dict): data to add
            ret_type (str, optional): return type. Defaults to "jsonable". Should be one of ["jsonable", "original"].

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
            message = "Add data to MongoDB successfully"
            data = self.__dict__[schema_name].add_data(input_data, ret_type)
        except Exception as e:
            indicator = False
            message = "[MongoHandler] Add data to MongoDB failed: {}".format(e)
            data = []
            logging.warning(message)
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": data}
    
    def update_data(self, schema_name: str, conditions: dict, update_data: dict, ret_type="jsonable") -> dict:
        """
        Update data in MongoDB and return the data updated
        
        Args:
            schema_name (str): name of schema
            conditions (dict): conditions of query (follow MongoDB query syntax)
            update_data (dict): data to update
            ret_type (str, optional): return type. Defaults to "jsonable". Should be one of ["jsonable", "original"].
        
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
            data = self.__dict__[schema_name].update_data(conditions, update_data, ret_type)
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
            indicator = True
            message = "Delete data from MongoDB successfully"
            delete_count = self.__dict__[schema_name].delete_data(conditions)
            if "_id" in conditions:
                conditions["_id"] = str(conditions["_id"])
            data = [{"deleted_count": delete_count, "conditions": conditions}]
        except Exception as e:
            indicator = False
            message = "[MongoHandler] Delete data from MongoDB failed: {}".format(e)
            data = []
            logging.warning(message)
        self.disconnect()
        return {"indicator": indicator, "message": message, "formatted_data": data}
    
class DocumentHandler(mongo.Document):
    meta = { "abstract": True}
    _valid_ret_type_list = ["jsonable", "original"]

    @classmethod
    def format_data(cls, mongo_obj, ret_type="jsonable"):
        sub_item = mongo_obj.to_mongo().to_dict()
        if ret_type not in cls._valid_ret_type_list:
            raise ValueError("Invalid ret_type: {}".format(ret_type))
        
        if ret_type == "jsonable":
            for key, value in sub_item.items():
                if isinstance(value, ObjectId):
                    sub_item[key] = str(value)
                elif isinstance(value, datetime.datetime):
                    sub_item[key] = value.timestamp()
        return sub_item

    @classmethod
    def format_data_list(cls, mongo_obj_list, ret_type="jsonable"):
        ret = []
        if ret_type not in cls._valid_ret_type_list:
            raise ValueError("Invalid ret_type: {}".format(ret_type))
        
        for item in mongo_obj_list:
            sub_item = item.to_mongo().to_dict()
            if ret_type == "jsonable":
                for key, value in sub_item.items():
                    if isinstance(value, ObjectId):
                        sub_item[key] = str(value)
                    elif isinstance(value, datetime.datetime):
                        sub_item[key] = value.timestamp()
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
    def handle_modified_time(cls, data: list) -> list:
        for item in data:
            item["modified_time"] = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
        return data

    @classmethod
    def get_headers(cls):
        return list(cls._fields_ordered)

    @classmethod
    def add_data(cls, data, ret_type="jsonable"):
        add_data = cls.format_and_validate_document(data)
        ret = cls.objects.insert(add_data)
        return cls.format_data_list(ret, ret_type)

    @classmethod
    def get_data(cls, conditions, order_by_list=[], ret_type="jsonable"):
        if conditions:
            if "_id" in conditions:
                conditions["_id"] = ObjectId(conditions["_id"])
            raw_data = cls.objects(__raw__=conditions)
        else:
            raw_data = cls.objects
        return cls.format_data_list(raw_data.order_by(*order_by_list), ret_type)
    
    @classmethod
    def update_data(cls, conditions, update_data, ret_type="jsonable"):
        if conditions:
            if "_id" in conditions:
                conditions["_id"] = ObjectId(conditions["_id"])
            raw_data = cls.objects(__raw__=conditions)
        else:
            raw_data = cls.objects
        return cls.format_data(raw_data.modify(__raw__={"$set": update_data}, new=True), ret_type)
    
    @classmethod
    def delete_data(cls, conditions):
        if conditions:
            if "_id" in conditions:
                conditions["_id"] = ObjectId(conditions["_id"])
            raw_data = cls.objects(__raw__=conditions)
        else:
            raw_data = cls.objects
        
        return raw_data.delete()

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

    def get_data(self, collection: str, conditions={}, order_by_list=[], ret_type="jsonable") -> dict:
        """
        Get data from MongoDB
        
        Args:
            collection (str): name of collection
            conditions (dict): conditions of query (follow MongoDB query syntax)
            order_by_list (list of tuple): order by. Elements of tuple are (key, order), 
                                           order is 1 for ascending and -1 for descending.
                                           e.g. [("modified_time", -1)]
            ret_type (str, optional): return type. Defaults to "jsonable". Should be one of ["jsonable", "original"].

        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "data": list
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
        return {"indicator": indicator, "message": message, "data": data}
    
    def _add_data(self, collection: str, data: list, ret_type="jsonable") -> dict:
        """
        Add data to MongoDB
        
        Args:
            collection (str): name of collection
            data (list of dict): data to add

        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "data": list
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
        return {"indicator": indicator, "message": message, "data": ret}

    def _update_data(self, collection: str, conditions: dict, update_data: dict, ret_type="jsonable") -> dict:
        """
        Update data in MongoDB
        
        Args:
            collection (str): name of collection
            conditions (dict): conditions of query (follow MongoDB query syntax)
            update_data (dict): data to update

        Returns:
            dict: {
                "indicator": bool,
                "message": str,
                "data": list
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
        return {"indicator": indicator, "message": message, "data": ret}

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
                "data": list
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
        return {"indicator": indicator, "message": message, "data": ret}

# if __name__ == '__main__':
#     # ========== MongoWidget Example ==========

#     configs = {
#                 "host": "localhost",
#                 "port": 48763,
#                 "database": "trigger",
#                 "username": "test",
#                 "password": "mongodb",
#               }
    
#     mw = MongoWidget(**configs)
#     res = mw.get_data("user", order_by_list=[("modified_time", -1)])
#     print(res["data"])

    # ========== MongoHandler Example ==========

    # Schema Example (Before using MongoHandler, you need to define schemas first):

    # class User(DocumentHandler):
    #     account_id = mongo.StringField(required=True)
    #     username = mongo.StringField(required=True)
    #     password = mongo.StringField(required=True)
    #     update_state = mongo.BooleanField(required=True, default=False)
    #     permission = mongo.StringField(required=True)
    #     favorite = mongo.ListField(field=mongo.DictField(), required=False)
    #     modified_time = mongo.StringField(required=True)
    #     comments = mongo.StringField(required=False, default="")
    #     meta = {"db_alias": "trigger"}

    # # Construct MongoHandler:

    # configs = {
    #             "alias": "trigger",
    #             "host": "localhost",
    #             "port": 48763,
    #             "database": "trigger",
    #             "username": "test",
    #             "password": "mongodb",
    #             "schemas": {"user": User}
    #           }
    # mh = MongoHandler(**configs)

    # # Get Data

    # conditions = {"update_state": False, "favorite": {"$elemMatch": {"title": "react"}}}
    # order_by_list = [("-modified_time")]
    # res = mh.get_data("user", conditions, order_by_list)
    # if res["indicator"]:
    #     print(res["data"])
    # else:
    #     print(res["message"])
    
#     # Add Data

#     data = [{
#             'account_id': 'charlie_sysadmin',
#             'username': 'Charlie SysAdmin',
#             'password': 'charlie_password',
#             'update_state': False,
#             'permission': 'a',
#             'favorite': [{'title': 'linux', 'author': 'open_source_guru'}],
#             "modified_time": datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3],
#             'comments': 'System administrator specializing in Linux.'
#             }]
#     res = mh.add_data("user", data)
#     if res["indicator"]:
#         print(res["data"])
#     else:
#         print(res["message"])
    
#     # Update Data

#     update_query = {"account_id": "charlie_sysadmin"}
#     update_data =  {"update_state": True}
#     res = mh.update_data("user", update_query, update_data)
#     if res["indicator"]:
#         print(res["data"])
#     else:
#         print(res["message"])
    
#     # Delete Data

#     delete_query = {"account_id": "charlie_sysadmin"}
#     res = mh.delete_data("user", delete_query)
#     if res["indicator"]:
#         print(res["data"])
#     else:
#         print(res["message"])