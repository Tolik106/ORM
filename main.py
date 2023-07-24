import psycopg2
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from classes import create_tables
from classes import create_tables, Publisher, Shop, Book, Stock, Sale


DSN = 'postgresql://postgres:McDonalds106@localhost:5432/book_stock'

engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind = engine)
session = Session()


with open('tests_data (1).json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()



session.close()