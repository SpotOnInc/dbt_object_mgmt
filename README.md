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
<details>
  <summary>command line and file parameters</summary>

  ##### command line
  ```bash
  dbt run-operation create_users \
    --vars "password: $3cr3t"
  ```

  ##### file parameters
  - **name** - the username for the user
  - **attributes** - array of properties as defined in the [Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/alter-user#object-properties-objectproperties)
  - **roles** - list of roles to grant to the user

</details>

___
### [create_network_policies](macros/create_network_policies.sql)

This macro helps with management of IP [network policies](https://docs.snowflake.com/en/user-guide/network-policies) - it will create or alter exising IP addresses with the ones provided in the corresponding files. If adding new IPs it is recommended that you document the associated services/ users.
<details>
  <summary>command line and file parameters</summary>

  ##### command line
  ```bash
  dbt run-operation create_network_policies
  ```

  ##### file parameters
  - **network_policy**: name for network policy
  - **comment**: a description for the network policy
  - **allowed_ip_list**: a list of iPv4 addresses that may access Snowflake with this policy; allows CIDR notation
  - **blocked_ip_list**: a list of iPv4 addresses that are blocked on this network policy

</details>

___
### [create_integration](./macros/create_integration.sql)

This macro helps with the creation and alteration of [integrations](https://docs.snowflake.com/en/sql-reference/sql/create-integration) that takes a 1:1 file to integration approach and is capable of creating api, notification, security, and/or storage integrations.
<details>
  <summary>command line and file parameters</summary>

  ##### command line
  ```bash
  dbt run-operation create_integration \
    --args 'file: ./snowflake/integration/s3_to_snowflake_integration.yml'
  ```
  ##### file parameters
  - **integration_name**: name for the integration
  - **integration_type**: the type of integration to create. can be one of:
    - `API, NOTIFICATION, SECURITY, STORAGE`
  - **enabled**: true | false
  - other optional paramters may be included by integration type:
    - [API](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#optional-parameters)
    - [Notification](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration)
    - [Security](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration)
    - [Storage](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration)

</details>


______
### [create_pipe](./macros/create_pipe.sql)

This macro helps with the (re)creation of snowpipes that takes a 1:1 file to snowpipe approach - it will create or replace the existing stage, table, and auto-ingesting snowpipe associated with the data in the individual files in your profiles target database. In order to run this for the first time you will need to create a [storage integration](./macros/create_integration.sql).
<details>
  <summary>command line and file parameters</summary>

  ##### command line
  ```bash
  dbt run-operation create_pipe \
    --args 'file: ./snowflake/snowpipe/s3_pipe_jaffle_shop_customers.yml'
  ```

  ##### file parameters
  - **integration_name**: the storage integration to use for loading
  - **schema_name**: the schema to load data into
  - **table_name**: the table to load data into
  - **s3_url**: the url of the bucket where files will be stored
  - **file_type**: file format that will be loaded. Has been tested with CSV and JSON, but can also be AVRO, ORV, PARQUET, or XML.
  - **columns**: an array with column names and their data types
  - **extra_format_options**: an array of formatting options for files to be loaded that differ from the default settings. These can be any within the [Format Type Options Documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#format-type-options-formattypeoptions) by file type.
  - **pattern**: a regex pattern to be used in the copy into statement.
    - to note - snowpipes will trim path segments from patterns in the snowpipe copy into statement. See more in the [Usage Notes Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-pipe#usage-notes).

</details>

___
### [grant_schema_access](./macros/grant_schema_access.sql)

This is helpful when sensitive data does not allow for granting select on future objects or to limit to certain environments ( i.e. not dev_ ). It is designed to be run after a `dbt run | build` command to grant access to roles in a project via the dbt graph object. It defaults to granting for model and seed resource types and can be extended to and of ['model', 'snapshot', 'test', 'seed', 'operation'].

<details>
  <summary>command line example</summary>

  ```bash
  dbt run-operation grant_schema_access \
    --args "roles: applications_read_only"
    # only models "{roles: applications_read_only, resource_types: ['models']}
  ```

</details>
