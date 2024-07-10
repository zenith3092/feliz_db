# Update History

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
