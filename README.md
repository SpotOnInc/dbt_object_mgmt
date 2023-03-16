This dbt repo is setup to house macros of snowflake object management with dbt.
Macros use a combination of snowflake put operations and stages to use local file data to create objects, with file paths set as environment variables. It contains the following macros, with example data formats in the [./snowflake](./snowflake/) folder.

These macros can be installed in a current dbt project with a few lines of setup in your project files:
```yml
# packages.yml
packages:
  - git: https://github.com/SpotOnInc/dbt_object_mgmt.git
    revision: main
```
```yml
# dbt_project.yml - these are default example paths
#                   set these to your actuals
vars:
  snowflake_user_file: snowflake/users/users.yml
  snowflake_network_policy_file: snowflake/policy/network_policies.yml
  snowflake_admin: SECURITYADMIN
```




---
### [create_users](./macros/create_users.sql)

This macro helps with the creation and management of snowflake users. Any new users that are in the users file will be created and existing will be altered. It is non-destructive and will only disable users with the `disabled` flag.

The variable `DRY_RUN` can be set to True, which will log statements without sending to snowflake for execution.

In order to not version control / store passwords in plain text, you will also need to pass in the variable `PASSWORD` which can be passed to your end users, and default to force change upon their first login.

Use the included convenience script [download_snowflake_users.py](./download_snowflake_users.sql) to download any existing users and setup `./snowflake/users/users.yml` which you may add any new users and their attributes to.

```bash
dbt run-operation create_users \
  --vars "PASSWORD: $3cr3t"
  # or --vars "{PASSWORD: $3cr3t, DRY_RUN: True}"
```

___
### [create_network_policy](macros/create_network_policy.sql)

This macro helps with management of whitelists - it will create or overwrite IP addresses exising with the ones provided in the corresponding files. If adding new IPs, it is also best practice to document the associated services/ users.

```bash
dbt run-operation create_network_policy # --vars "DRY_RUN: True"
```

___
### [create_pipe](./macros/create_pipe.sql)

This macro helps with the (re)creation of snowpipes that takes a single file approach - it will create or replace the existing stage, table, and auto-ingesting snowpipe associated with the data in the individual .yml files. In order to run this for the first time you will need to create a [storage integration](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html#syntax) which is not an automated procedure at this time.

```bash
dbt run-operation create_pipe \
  --args "$(cat ./snowflake/snowpipe/s3_pipe_jaffle_shop_orders.yml)"  \
  # add --vars "DRY_RUN: False" to execute
```

___
### [grant_schema_access](./macros/grant_schema_access.sql)

This is helpful when sensitive data does not allow for granting select on future objects or to limit to certain environments ( i.e. not dev_ ). It is designed to be run after a `dbt run | build` command to grant access to roles in a project via the dbt graph object. It defaults to granting for model and seed resource types and can be extended to and of ['model', 'snapshot', 'test', 'seed', 'operation'].

```bash
dbt run-operation grant_schema_access \
  --args "roles: applications_read_only"
  # only models "{roles: applications_read_only, resource_types: ['models']}
```
