# feliz_db

An ODM / ORM designed to assist in querying data from some kinds of database

## Environment

-   Python 3.9 or higher

## Installation

```bash
pip install feliz_db
```

# Postgres Tools

## PostgresHandler

### Initialization

```python
from feliz_db.postgres_tools import PostgresHandler

configs = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "username": "postgres",
    "password": "postgres"
}

DH = PostgresHandler(**configs)
```

Users can use the `add_data`, `get_data`, `update_data`, and `delete_data` methods to add, update, and delete data from a table.

All methods will return a dictionary with the following keys:

-   indicator: The indicator of the operation. It will be `True` if the operation is successful, otherwise it will be `False`.
-   message: The message of the operation. It will be a string.
-   data: The data of the operation. It will be a list of tuples.
-   formatted_data: The formatted data of the operation. It will be a list of dictionaries.

### Add data

```python
adding_list = [{"column_name": "value"}]
add_res = DH.add_data(table="table_name", adding_list=adding_list)
```

### Get data

```python
conditions = [("column_name =", "value")]
get_res = DH.get_data(table="table_name", conditional_rule_list=conditions)
```

### Update data

```python
update_list = [{"_id": 1,"column_name": "value1"},
               {"_id": 2,"column_name": "value2"}]
update_res = DH.update_data(table="table_name",editing_list=update_list, reference_column_list=["_id"])
```

### Delete data

```python
delete_list = [{"_id": 1},
               {"_id": 2}]
delete_res = DH.delete_data(table="table_name", filter_list=delete_list, reference_column_list=["_id"])
```

## PostgresModelHandler & PostgresField

### Define a schema model

User can create a model by inheriting the `PostgresModelHandler` class and defining the fields by `PostgresField` class.

```python
from feliz_db.postgres_tools import PostgresModelHandler

class TestSchema(PostgresModelHandler):
    meta = {"initialize": True,
            "conditional_init": True,
            "init_type": "schema",
            "schema_name": ["test"]
    }
```

### Create a schema

```python
configs = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "username: "postgres",
    "password": "postgres"
}

DH = PostgresHandler(**configs)

TestSchema.execute_sql(DH, TestSchema.form_schema_sql)
```

### Define a table model

User can create a model by inheriting the `PostgresModelHandler` class and defining the fields by `PostgresField` class.

```python
from feliz_db.postgres_tools import PostgresModelHandler, PostgresField

class UserListTable(PostgresModelHandler):
    _id           = PostgresField(serial=True, primary_key=True)
    modified_time = PostgresField("TIMESTAMP")
    user_id       = PostgresField("VARCHAR(20)", unique=True)
    full_name     = PostgresField("TEXT")
    password      = PostgresField("TEXT", default="''")
    comments      = PostgresField("TEXT", default="''")

    meta = {"initialize": True,
            "conditional_init": True,
            "init_type": "table",
            "schema_name": ["test"],
            "table_name": ["user_list"]}

    @classmethod
    def if_initialize(cls, schema_name, table_name):
        now = data_time.datetime.now()
        sql = f"""
        INSERT INTO {schema_name}.{table_name} (modified_time, user_id, full_name, password, comments)
            VALUES ( '{now}', 'admin', 'Administrator', '$2b$12$pDY0sUa6X4AB0kWySLY8lO2ioQtarEWoSeE0kUvg1bZEXMPXefcv.', '');
        """
        return sql

    @classmethod
    def else_initialize(cls, schema_name, table_name):
        return ""
```

### Create a table

```python
configs = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "username: "postgres",
    "password": "postgres"
}

DH = PostgresHandler(**configs)

UserListTable.execute_sql(DH, UserListTable.form_table_conditional_sql)
```

### Multiple create schema and table

All models should execute the `create_sql` method and then use any model to execute the `execute_sql` method to create the schema and table.

After creating the schema and table, the user can execute the `clear_sql` method to clear the schema and table.

Note: `execute_sql` and `clear_sql` can be executed by any model. Only the `create_sql` method should be executed by all models.

```python
configs = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "username": "postgres",
    "password": "postgres"
}
DH = PostgresHandler(**configs)

TestSchema.create_sql()
UserListTable.create_sql()
UserListTable.execute_sql(DH)
UserListTable.clear_sql()
```

# Mongo Tools

## DocumentHandler

DocumentHandler is a class that can be used to define a schema and table for a MongoDB database. The user can create a model by inheriting the `DocumentHandler` class and defining the fields by the `mongoengine` class.

```python
from mongoengine import *
from feliz_db.mongo_tools import DocumentHandler

class ConfigCameras(DocumentHandler):
    item_id       = StringField(required=True, unique=True)
    item_name     = StringField(required=True)
    modified_time = DateTimeField(required=True)

    class Coordinates(EmbeddedDocument):
        dashboard = ListField(FloatField(), required=True)
    coordinates   = EmbeddedDocumentField(Coordinates)

    class Rtsp(EmbeddedDocument):
        username  = StringField(required=True)
        password  = StringField(required=True)
        params    = StringField(required=True)
        rtsp_url  = StringField(required=True)
    rtsp          = EmbeddedDocumentField(Rtsp)

    class Projection(EmbeddedDocument):
        source    = ListField(ListField(FloatField()))
        target    = ListField(ListField(FloatField()))
    projection    = EmbeddedDocumentField(Projection)

    comments      = DictField()

    meta = {"db_alias": "dt_config", "collection": "cameras"}
```

## MongoHandler

### Initialization

```python
from feliz_db.mongo_tools import MongoHandler

configs = {
    "host": "localhost",
    "port": 5445,
    "database": "mongo",
    "username": "mongo",
    "password": "mongo",
    "schemas": {"cameras": ConfigCameras}
}

DH = MongoHandler(**configs)
```

MongoHandler will automatically create the model and then users can use the `add_data`, `get_data`, `update_data`, and `delete_data` methods to add, update, and delete data from a collection.

All methods will return a dictionary with the following keys:

-   indicator: The indicator of the operation. It will be `True` if the operation is successful, otherwise it will be `False`.
-   message: The message of the operation. It will be a string.
-   formatted_data: The formatted data of the operation. It will be a list of dictionaries.

### Add data

```python
add_res = DH.add_data("cameras", [{...}, {...}])
```

### Get data

```python
get_res = DH.get_data("cameras", {"rtsp.username": "admin"})
```

### Update data

```python
update_res = MH.update_data("cameras", {"item_id": "123"}, {"item_name": "ABC"})
```

### Delete data

```python
delete_res = MH.delete_data("cameras", {"item_id": "123"})
```

## MongoWidget

MongoWidget is a class that can be used to query data from a MongoDB database. The user can use the `get_data` method to query data from a collection.

MongoWidget also provides the `add_data`, `update_data`, and `delete_data` methods to add, update, and delete data from a collection. However, these methods are not recommended for use because they are not safe. It is recommended to use the class `MongoModelHandler` to define a schema and table to add, update, and delete data.

```python
from feliz_db.mongo_tools import MongoWidget

configs = {
    "host": "localhost",
    "port": 5445,
    "database": "mongo",
    "username": "mongo",
    "password": "mongo",
}
MW = MongoWidget(**configs)

get_res = MW.get_data("cameras", {"rtsp.username": "admin"})
```
