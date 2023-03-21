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
  dry_run: true # set this to false to execute commands
```

---
### [create_users](./macros/create_users.sql)

This macro helps with the creation and management of snowflake users. Any new users that are in the users file will be created and existing will be altered. It is non-destructive and will only disable users with the `disabled` flag.

In order to not version control / store passwords in plain text, you should also pass in the variable `password` which can be passed to your end users, and default to force change upon their first login.

Use the included convenience script [download_snowflake_users.py](./download_snowflake_users.sql) to download any existing users and setup `./snowflake/users/users.yml` which you may add any new users and their attributes to.

```bash
dbt run-operation create_users \
  --vars "password: $3cr3t"
```

___
### [create_network_policies](macros/create_network_policies.sql)

This macro helps with management of IP [network policies](https://docs.snowflake.com/en/user-guide/network-policies) - it will create or alter exising IP addresses with the ones provided in the corresponding files. If adding new IPs it is recommended that you document the associated services/ users.

```bash
dbt run-operation create_network_policies
```

___
### [create_integration](./macros/create_integration.sql)

This macro helps with the creation and alteration of [integrations](https://docs.snowflake.com/en/sql-reference/sql/create-integration) that takes a 1:1 file to integration approach and is capable of creating api, notification, security, and/or storage integrations.

```bash
dbt run-operation create_integration \
  --args 'file: ./snowflake/integration/s3_to_snowflake_integration.yml'
```

______
### [create_pipe](./macros/create_pipe.sql)

This macro helps with the (re)creation of snowpipes that takes a 1:1 file to snowpipe approach - it will create or replace the existing stage, table, and auto-ingesting snowpipe associated with the data in the individual files. In order to run this for the first time you will need to create a [storage integration](./macros/create_integration.sql).

```bash
dbt run-operation create_pipe \
  --args 'file: ./snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml'
```

___
### [grant_schema_access](./macros/grant_schema_access.sql)

This is helpful when sensitive data does not allow for granting select on future objects or to limit to certain environments ( i.e. not dev_ ). It is designed to be run after a `dbt run | build` command to grant access to roles in a project via the dbt graph object. It defaults to granting for model and seed resource types and can be extended to and of ['model', 'snapshot', 'test', 'seed', 'operation'].

```bash
dbt run-operation grant_schema_access \
  --args "roles: applications_read_only"
  # only models "{roles: applications_read_only, resource_types: ['models']}
```
