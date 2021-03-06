import os

from sqlalchemy import create_engine

user = os.getenv('C9_USER')
password = ''
database = 'findb'
local_engine = create_engine('mysql+mysqlconnector://{}:{}@127.0.0.1/{}'.format(user, password, database),
                             encoding='utf-8')
