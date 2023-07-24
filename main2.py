import psycopg2
import json
import sqlalchemy
from sqlalchemy import sql
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


def get_shops(publisher_name):
    publisher = session.query(Publisher).filter(Publisher.name == publisher_name or Publisher.id == publisher_name).first()
    if publisher:
        query = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).\
            join(Stock, Book.id == Stock.id_book).\
            join(Sale, Stock.id == Sale.id_stock).\
            join(Shop, Stock.id_shop == Shop.id).\
            filter(Book.id_publisher == publisher.id)
        for title, shop_name, price, date_sale in query:
            print(f"{title} | {shop_name} | {price} | {date_sale}")
    else:
        print("Автор не найден")

if __name__ == '__main__':
    с = input(f'Введите id или имя автора')
    get_shops(publisher_name=с)



session.close()