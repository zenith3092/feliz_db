# Update History

## v0.0.7

### Update `_id` condition

Automatically change the `_id` condition to `ObjectId` when the `_id` is a string. This feature also support the conditions which contain mongodb operators.

### Update return value

All the `add_data`, `get_data`, `update_data` and `delete_data` methods uniformly at least return the three keys : `indicator`, `message` and `formatted_data`.
