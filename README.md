This sample dbt repo is setup to house examples of snowflake object management with _dbt.
It contains the following macros, with example data in the [./snowflake](./snowflake/) folder.



### [manage_snowflake_users](./macros/manage_snowflake_users.sql)

This macro helps with the creation and management of snowflake users. Any new users that are in the users file will be created and existing will be updated. It is non-destructive and will only disable users with the `disabled` flag.

The variable `DRY_RUN` will default to True, which will log statements without sending to snowflake for execution.

In order to not version control / store passwords in plain text, you will also need to pass in the variable `PASSWORD` which can be passed to your end users, and changed upon their first login.

First timers can use the script [download_snowflake_users.py](./download_snowflake_users.sql) to download any existing users and setup `./snowflake/users/users.yml` which you may add any new users and their attributes to. Once set up you can run:
```bash
dbt run-operation manage_snowflake_users \
  --args "$(cat ./snowflake/users/users.yml)" \
  --vars '{PASSWORD: $3cr3t}' # add {DRY_RUN: False} to execute
```

___
### [create_whitelist](macros/create_whitelist.sql)

This macro helps with management of whitelists and takes a multiple file approach - it will overwrite exising with the ip addresses included in the .yml files. If adding new IPs, it is also best practivce to document the associated services/ users.

```bash
dbt run-operation create_whitelist \
  --args "$(cat ./snowflake/whitelist/{file_name}.yml)" \
  # add --vars {DRY_RUN: False} to execute
```

___
### [create_pipe](./macros/create_pipe.sql)

This macro helps with the (re)creation of snowpipes that takes a single file approach - it will create or replace the existing stage, table, and auto-ingesting snowpipe associated with the data in the individual .yml files. In order to run this for the first time you will need to create a [storage integration](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html#syntax) which is not an automated procedure at this time.

```bash
dbt run-operation create_pipe \
  --args "$(cat ./snowflake/snowpipe/s3_pipe_jaffle_shop_orders.yml)"  \
  # add --vars {DRY_RUN: False} to execute
```

___
### [grant_schema_access](./macros/grant_schema_access.sql)

This is helpful when sensitive data does not allow for granting future objects or to limit to certain environments ( i.e. not dev_ ). It is designed to be run after a `dbt run | build` command to grant access to roles in a projecct via the dbt graph object.  

```bash
dbt run-operation grant_schema_access \
  --args "roles: applications_read_only"
```

___

### _dbt resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
