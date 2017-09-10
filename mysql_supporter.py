import os
from sqlalchemy import create_engine, exc, types

user = os.getenv('C9_USER')
password = ''
database = 'findb'
local_engine = create_engine('mysql+mysqlconnector://{}:{}@localhost/{}'.format(user, password, database),
                             encoding='utf-8')
