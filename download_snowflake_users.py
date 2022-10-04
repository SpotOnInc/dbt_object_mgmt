import snowflake.connector
from dotenv import load_dotenv
import yaml
import os
load_dotenv()


ctx = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    role='sysadmin'
)

with ctx.cursor() as cs:
    cs.execute(f"use warehouse {os.environ['SNOWFLAKE_WAREHOUSE']}")

    cs.execute("""
    select  
      name
      , login_name
      , display_name
      , first_name
      , last_name
      , email
      , comment
      , to_boolean(disabled) as disabled
      , default_warehouse
      , default_namespace
      , default_role
    from snowflake.account_usage.users
    where deleted_on is null
    and name != 'SNOWFLAKE'
    order by 1
    """)
    query_results = cs.fetchall()
    snowflake_users = query_results
    users_columns = [x.name.lower() for x in cs.description]

    cs.execute("""
    select
      role
      , grantee_name
    from snowflake.account_usage.grants_to_users
    where deleted_on is null
    order by 2, 1
    """)
    query_results = cs.fetchall()
    snowflake_grants = query_results
    grants_columns = [x.name.lower() for x in cs.description]

    cs.close()


user_list = [dict(zip(users_columns, user)) for user in snowflake_users]
grants_list = [dict(zip(grants_columns, grant)) for grant in snowflake_grants]

# define attributes
user_attribute_list = [
    'login_name',
    'display_name',
    'first_name',
    'last_name',
    'email',
    'comment',
    'disabled',
    'default_warehouse',
    'default_namespace',
    'default_role',
]

set_attr_list = {}
set_attr_list['users'] = [
    {'name': user['name'],
     'attributes': {
        attr: user[attr] for attr in user_attribute_list if user[attr] is not None
        },
     'roles': [grant['role'] for grant in grants_list if grant['grantee_name'] == user['name']]
    }
    for user in user_list
]


# this fixes unindented yaml files - while technically correct, it's not pretty
# https://stackoverflow.com/a/39681672
class MyDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)


with open('./snowflake/users/users.yml', 'w') as w:
    yaml.dump(set_attr_list, w, Dumper=MyDumper, sort_keys=False)
