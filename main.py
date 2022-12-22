import asyncio
import datetime

from sqlalchemy import create_engine, MetaData, Table, Text, Column, Integer, Float, text, Date

from moysklad import Moysklad
from config import LOGIN, PASSWORD, DATABASE_PATH


def moysklad_stock_fetching(data):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()
    insert_data = []  # Data which will be inserted into table
    products = Table('moysklad_stock', metadata,
                     Column('date', Date()),
                     Column('code', Text()),
                     Column('article', Text()),
                     Column('supplier', Text()),
                     Column('product_name', Text()),
                     Column('unit_name', Text()),
                     Column('units_aval', Integer()),
                     Column('units_reserve', Integer()),
                     Column('units_wait', Integer()),
                     Column('units_stock', Integer()),
                     Column('cost', Float()),
                     Column('cost_sum', Float()),
                     Column('price', Float()),
                     Column('price_sum', Float()),
                     Column('stock_days', Float()),
                     Column('ext_code', Text()),
                     Column('category_level_1', Text()),
                     Column('category_level_2', Text())
                     )
    metadata.create_all(engine)
    for el in data:

        insert_data.append(el)

        if len(insert_data) == 1000:
            ins = products.insert().values(insert_data)
            conn.execute(ins)
            insert_data = []

    if len(insert_data) > 0:
        ins = products.insert().values(insert_data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def moysklad_loss_fetching(data):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()
    final_data = []
    loss = Table('moysklad_loss', metadata,
                 Column('date', Date()),
                 Column('time', Text()),
                 Column('loss_id', Text()),
                 Column('ext_code', Text()),
                 Column('status', Text()),
                 Column('stock', Text()),
                 Column('article', Text()),
                 Column('product_name', Text()),
                 Column('num', Integer()),
                 Column('reason', Text()),
                 Column('price', Float()),
                 Column('comment', Text()),
                 Column('supplier', Text()),
                 Column('category_level_1', Text()),
                 Column('category_level_2', Text()),
                 Column('qr_sum', Float()),
                 )

    metadata.create_all(engine)
    sql = text('DELETE FROM moysklad_loss')
    engine.execute(sql)
    for i in range(len(data)):
        final_data.append(data[i])
        if i % 1000 == 0:
            ins = loss.insert().values(final_data)
            conn.execute(ins)
            final_data = []
    if len(final_data) > 0:
        ins = loss.insert().values(final_data)
        conn.execute(ins)

    conn.close()
    engine.dispose()


def moysklad_revenue_fetching(products_json):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    products = Table('moysklad_revenue', metadata,
                     Column('ext_code', Text()),
                     Column('date', Date()),
                     Column('time', Text()),
                     Column('product_name', Text()),
                     Column('administrator', Text()),
                     Column('counterparty_name', Text()),
                     Column('category_level_1', Text()),
                     Column('category_level_2', Text()),
                     Column('code', Text()),
                     Column('article', Text()),
                     Column('barcode', Text()),
                     Column('discount', Integer()),
                     Column('quantity', Integer()),
                     Column('price', Float()),
                     Column('cash', Float()),
                     Column('non_cash', Float()),
                     Column('supplier', Text()),
                     Column('qr_sum', Float()),
                     )

    metadata.create_all(engine)
    sql = text('DELETE FROM moysklad_revenue')
    engine.execute(sql)
    for el in products_json:
        ins = products.insert().values(el)
        conn.execute(ins)
    conn.close()
    engine.dispose()


async def main():
    while True:
        moysklad = Moysklad(LOGIN, PASSWORD)
        time_start = datetime.datetime.now()
        print('Start parsing moysklad_revenue')
        try:
            moysklad_revenue = await moysklad.get_demands()
            moysklad_revenue.extend(await moysklad.get_retail_demands())
            print('Start fetching data to moysklad_revenue_table')
            moysklad_revenue_fetching(moysklad_revenue)
        except Exception as error:
            print("moysklad_revenue parsing errored")
            print(error)

        await asyncio.sleep(10)

        print('Start parsing moysklad_loss')
        try:
            moysklad_loss = await moysklad.get_losses()
            print('Start fetching data to moysklad_loss_table')
            moysklad_loss_fetching(moysklad_loss)
        except Exception as error:
            print("moysklad_loss parsing errored")
            print(error)

        await asyncio.sleep(10)

        print('Start parsing moysklad_stock')
        try:
            moysklad_stock = await moysklad.get_stocks()
            print('Start fetching data to moysklad_stock_table')
            moysklad_stock_fetching(moysklad_stock)
        except Exception as error:
            print("moysklad_stock parsing errored")
            print(error)
        print('TIME:', datetime.datetime.now() - time_start)
        await moysklad.end_of_parsing()

if __name__ == '__main__':
    asyncio.run(main())
