# Update History

## v0.2.0

## Update `PostgresField`

### Add `customized_field` parameter

If programmers want to customize the field of the column, they can use the `customized_field` parameter. If `customized_field` is set, all the other parameters will be ignored.

### Add `get_field` method

Programmers could get the field of the column by using the `get_field` method.

## Update `PostgresModelHandler`

### Add `other_conditions_sql` in `meta`

If programmers want to declare the constraints of the table, they can use the `other_conditions_sql` parameter in the `meta`. The `other_conditions_sql` will be added to the end of the header declaration.

### Add `customized_sql` in `meta`

If programmers want to add the customized SQL statement after the creation of the table, they can use the `customized_sql` parameter in the `meta`. This parameter is useful when programmers want to declare the partition of the table.

## Update `PostgresEnum`

### Add `mapping_value` parameter

If programmers want to map the value of the enum to the other value, they can use the `mapping_value` parameter.

## v0.1.0

## Update `MongoHandler`

In old version, all the transaction of `MongoHandler` is inclusive of the auto connection and disconnection. However, this design lowers the efficiency of the program. In this version, the `MongoHandler` is updated to support the auto connection but manual disconnection. The `MongoHandler` will automatically connect to the database when the instance is created. But the disconnection is not automatically. Programmers need to manually disconnect the database by using the `disconnect` method.

## v0.0.13

### Update `PostgresModelHandler`

#### Update `meta` of enum

In old versions, the schema name of an enum is fixed to `public`. Now, the schema name of an enum can be customized. Just set the `schema_name` in the `meta` of the enum.

#### Update `meta` of table (Add unique constraint)

In old versions, the unique constraint of a table is not supported. Now, the unique constraint of a table is supported. Just set the `unique` in the `meta` of the table.

Example:

```python
meta = {
    "unique_constraint": [("column_name1", "column_name2"), ("column_name2", "column_name3")]
}
```

The above example will create the unique constraint of the table on the columns `column_name1` and `column_name2`, and the columns `column_name2` and `column_name3`. Namely, the sql statement will be like this:

```sql
CREATE TABLE public.test_table (_id SERIAL PRIMARY KEY, column_name1 integer NOT NULL, column_name2 integer NOT NULL, column_name3 integer NOT NULL, UNIQUE(column_name1, column_name2), UNIQUE(column_name2, column_name3));
```

(ignore the type of the columns)

#### Update `update_data` of `MongoHandler` (Add customized parameter)

The default query condition of `update_data` is a dictionary with the key `$set`. However, sometimes, programmers may want to use the other operators like `$inc`, `$mul`, etc. Now, the `update_data` method can accept the dictionary with the other operators with the parameter `customized` set to `True`.

#### Update `update_data` of `MongoHandler` (Update the method of updating)

In old versions, the return value of `update_data` is a dictionary with the key `indicator`, `message`, and `formatted_data`. And the `formatted_data` is generated by the `modify` method of `mongoengine.queryset.queryset.QuerySet`. However, this method will only update one document even if the query condition can match multiple documents. To fix this issue, the `update_data` use the method `update` of `mongoengine.queryset.queryset.QuerySet` to update the documents. But the return value of the `update` method is not the same as the `modify` method. It returns the count of the updated documents. So, the `formatted_data` is changed to the count of the updated documents instead of the updated documents.

## v0.0.12

### Update `PostgresHandler`

#### Exception handling

If using the following methods of `PostgresHandler`:

-   `add_data`
-   `get_data`
-   `update_data`
-   `delete_data`
    and there is an error in the process, the program will raise an exception.

In this version, the exception message contains the table name.

### Add pseudo class attribute in `PostgresModelHandler`

For using `PostgresEnum` more conveniently, two pseudo class attributes are added to `PostgresModelHandler`:

-   `key`
-   `value`

The two can be used to type the `PostgresEnum` value more conveniently.

```python
from feliz_db.postgres_tools import PostgresModelHandler, PostgresEnum

class TestEnum(PostgresModelHandler):
    A = PostgresEnum("1")
    B = PostgresEnum("2")
    C = PostgresEnum("3")

    meta = {
        "init_type": "enum",
        "enum_name": "test"
    }

def test_function(test_enum: TestEnum):
    print(test_enum.key)
    print(test_enum.value)

test_function(TestEnum.A) # "A", "1"
```

## v0.0.11

### Update `meta`

#### New features

-   init_index (bool): If `True`, the index will be initialized at the first time the table is created.
-   init_type (str): Previously, the options of `init_type` were `schema` and `table`. Now, new option `enum` is available.
-   enum_name (str or list): The name of the enum type. This parameter is required when the `init_type` is `enum`.

#### Update features

-   initialize (bool): Now has a default value of `False`. Programmers do not need set it if they do not want to initialize the database.
-   conditional_init (bool): Now has a default value of `False`. Programmers do not need set it if they do not want to conditionally initialize the database.
-   schema_name, table_name, enum_name (str or list): Now can accept a list of strings or a pure string. But the length of the `table_name` and `enum_name` should be 1.
-   authorization (str): Previously, if the `init_type` was `schema`, the `authorization` was the required parameter. Now, the `authorization` is not required. The default value is `None`. This change is to make the `authorization` parameter more flexible. If programmers want to let the `PostgresInitialware` in `feliz` to automatically set the `authorization`, they can set the `authorization` to `None`. If programmers want to set the `authorization` by themselves, they can set the `authorization` to the value they want.

### New model field - `PostgresEnum`

In light of the rising popularity of the `Enum` library in Python and adding the `Enum` value into database, a new model field `PostgresEnum` is added. Now, programmers can use the `PostgresEnum` field to define the `Enum` type in the database.

Namely, they can limit the value of a column to the `Enum` values. If the input value is not in the class of `PostgresEnum`, the program will return a false indicator.

```python
from feliz_db.postgres_tools import PostgresModelHandler, PostgresEnum, PostgresHandler

class TestEnum(PostgresModelHandler):
    A = PostgresEnum("1")
    B = PostgresEnum("2")
    C = PostgresEnum("3")

    meta = {
        "initialize": True,
        "init_type": "enum",
        "enum_name": "test"
    }
```

#### Update `PostgresField`

Based on the new feature of `PostgresEnum`, the `PostgresField` is updated. Now, the `PostgresField` can accept the `PostgresEnum` type. Programmer can import the `PostgresEnum` class through the parameter `enum_class`.

```python

from feliz_db.postgres_tools import PostgresModelHandler, PostgresField, PostgresEnum

class TestEnum(PostgresModelHandler):
    A = PostgresEnum("1")
    B = PostgresEnum("2")
    C = PostgresEnum("3")

    meta = {
        "initialize": True,
        "init_type": "enum",
        "enum_name": "test"
    }

class TestTable(PostgresModelHandler):
    _id        = PostgresField(serial=True, primary_key=True)
    test_level = PostgresField(enum_class=TestEnum)

    meta = {
        "init_type": "table",
        "schema_name": "public",
        "table_name": "test_table",
    }
```

#### Usage of `PostgresHandler`

If the column of a table is defined as some of enum type, the postgres database will inspect whether the input value is valid.

```python
DH = PostgresHandler(**configs)

# This will work
r1 = DH.add_data(table="test_table", adding_list=[{"test_level": TestEnum.A.value}])

# This will return false indicator
r2 = DH.add_data(table="test_table", adding_list=[{"test_level": "error"}])
```

#### Restore the enum data type

The `PostgresModelHandler` can now restore the enum data type. Just use the method `restore_enum_data`.

```python
get_res = DH.get_data(table="test_table")
print(get_res["formatted_data"]) # [{"_id": 1, "test_level": "1"}]

restored_data = TestTable.restore_enum_data(get_res["formatted_data"])
print(restored_data) # [{"_id": 1, "test_level": PostgresEnum(TestEnum, 1)]
```

#### The characteristics of `PostgresEnum` class

Before Introduction of `PostgresEnum` class, creating two class `TestEnum` and `OtherEnum` as follows:

```python
class TestEnum(PostgresModelHandler):
    A = PostgresEnum("1")
    B = PostgresEnum("2")
    C = PostgresEnum("3")

    meta = {
        "initialize": True,
        "init_type": "enum",
        "enum_name": "test"
    }

class OtherEnum(PostgresModelHandler):
    A = PostgresEnum("1")
    B = PostgresEnum("2")
    C = PostgresEnum("3")

    meta = {
        "initialize": True,
        "init_type": "enum",
        "enum_name": "other"
    }
```

The `PostgresEnum` class has two attributes: `key` and `value`. The `key` is the name of the enum value, and the `value` is the value of the enum value.

```python
print(TestEnum.A.key)            # "A"
print(TestEnum.A.value)          # "1"
```

The `PostgresEnum` class has a method `__eq__` to compare the value of the enum value. In addition, If the two same enum values or enum keys are from different classes, the comparison will return `False`.

```python
print(TestEnum.A == TestEnum.A)  # True
print(TestEnum.A == TestEnum.B)  # False
print(TestEnum.A == OtherEnum.A) # False
```

The belonging of the `PostgresEnum` class can be checked by the operator `in`.

```python
print(TestEnum.A in TestEnum)    # True
print(TestEnum.A in OtherEnum)   # False
```

If programmers define the duplicated enum keys or values in the same class, the program will raise an error.

```python
class TestEnum(PostgresModelHandler):
    A = PostgresEnum("1")
    B = PostgresEnum("1") # ValueError: Duplicate enum value: 1
    C = PostgresEnum("3")

    meta = {
        "initialize": True,
        "init_type": "enum",
        "enum_name": "test"
    }
```

```python
class TestEnum(PostgresModelHandler):
    A = PostgresEnum("1")
    A = PostgresEnum("2") # ValueError: Duplicate enum key: A

    meta = {
        "initialize": True,
        "init_type": "enum",
        "enum_name": "test"
    }
```

## v0.0.10

### Fix the bug

A bug of `from_table_format` method is fixed. Now can normally get the list of data from the table format.

## v0.0.9

### Fix the bug of `PostgresHandler`

A bug of `get_header` method is fixed. Now can fit the condition of no serial primary key.

### `customized_sql` parameter of `PostgresField`

Programmers can use `customized_sql` parameter to customize the SQL statement of the column.

```python
from feliz_db.postgres_tools import PostgresModelHandler, PostgresField

class TestTable(PostgresModelHandler):
    _id        = PostgresField(serial=True, primary_key=True)
    test_level = PostgresField(customized_sql="integer NOT NULL CHECK (test_level > 0)")

    meta = {
        "init_type": "table",
        "schema_name": ["public"],
        "table_name": ["test_table"],
    }

print(TestTable.form_table_sql())
"""
CREATE TABLE public.test_table (_id SERIAL PRIMARY KEY, test_level integer NOT NULL CHECK (test_level > 0));
"""
```

## v0.0.7

### Update `_id` condition

Automatically change the `_id` condition to `ObjectId` when the `_id` is a string. This feature also support the conditions which contain mongodb operators.

### Update return value

All the `add_data`, `get_data`, `update_data` and `delete_data` methods uniformly at least return the three keys : `indicator`, `message` and `formatted_data`.
