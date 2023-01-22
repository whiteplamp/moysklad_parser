import base64
import datetime

import aiohttp
import asyncio

import requests


def get_auth_header(login, password):
    message = login + ':' + password
    login_bytes = message.encode('ascii')
    base64_bytes_login = base64.b64encode(login_bytes)
    base64_message = base64_bytes_login.decode('ascii')
    return f'Basic {base64_message}'


class Moysklad:
    def __init__(self, login, password):
        self.auth_header = {'Authorization': get_auth_header(login, password)}
        connector = aiohttp.TCPConnector(limit=5)
        self.session = aiohttp.ClientSession(headers=self.auth_header, connector=connector)

    async def get_retail_demands(self):
        retail_demand_url = 'https://online.moysklad.ru/api/remap/1.2/entity/retaildemand'
        urls_data = self.get_urls_retail_demand_data(retail_demand_url)
        data_from_urls = []
        final_data = []
        session = self.session
        count = 0
        for obj in urls_data:
            for el in obj:
                data = obj[el]
                positions_response = await session.get(data['positions'])
                positions_json = await positions_response.json()
                counterparty_name_response = await session.get(data['counterparty_name'])
                counterparty_name_json = await counterparty_name_response.json()
                administrator_response = await session.get(data['administrator'])
                administrator_json = await administrator_response.json()
                try:
                    name = counterparty_name_json['name']
                except Exception:
                    await asyncio.sleep(5)
                    counterparty_name_response = await session.get(data['counterparty_name'])
                    counterparty_name_json = await counterparty_name_response.json()
                    print('COUNTERPARTY NAME ERRORED')
                    await asyncio.sleep(2)
                try:
                    name = administrator_json['name']
                except Exception:
                    await asyncio.sleep(5)
                    administrator_response = await session.get(data['administrator'])
                    administrator_json = await counterparty_name_response.json()
                    print('ADMIN NAME ERRORED')
                    await asyncio.sleep(2)

                count += 3
                if count % 45 == 0:
                    await asyncio.sleep(2)
                data_from_urls.append({
                    el: {
                        'ext_code': data['ext_code'],
                        'positions': positions_json,
                        'administrator': administrator_json['name'],
                        'counterparty_name': counterparty_name_json['name'],
                        'date': data['date'],
                        'time': data['time'],
                        'cashSum': data['cashSum'],
                        'noCashSum': data['noCashSum'],
                        'qrSum': data['qrSum'],
                    }
                })
        for url in data_from_urls:
            for el in url:
                data = url[el]
                for obj in data['positions']['rows']:
                    assortment = await session.get(obj['assortment']['meta']['href'])
                    assortment = await assortment.json()
                    count += 1
                    if count % 45 == 0:
                        await asyncio.sleep(2)
                    obj['product'] = assortment
        for url in data_from_urls:
            for el in url:
                data = url[el]
                for obj in data['positions']['rows']:
                    supplier_data = True
                    try:
                        supplier_url = obj['product']['supplier']['meta']['href']
                    except KeyError:
                        try:
                            supplier_response = await session.get(obj['product']['product']['meta']['href'])
                            supplier_json = await supplier_response.json()
                            supplier_url = supplier_json['supplier']['meta']['href']
                            obj['product']['pathName'] = supplier_json['pathName']
                        except:
                            supplier_url = ''
                            supplier_data = False
                    if supplier_data:
                        supplier = await session.get(supplier_url)
                        supplier = await supplier.json()

                        count += 1
                        if count % 45 == 0:
                            await asyncio.sleep(1)
                        obj['product']['supplier'] = supplier
        for el in data_from_urls:
            for obj in el:
                data = el[obj]
                date = data['date']
                time = data['time']
                counterparty_name = data['counterparty_name']
                administrator = data['administrator']
                cash_sum = float(data['cashSum']) / 100
                non_cash_sum = float(data['noCashSum']) / 100
                qr_sum = float(data['qrSum']) / 100

                for position in data['positions']['rows']:
                    product = position['product']
                    product_name = product['name']
                    try:
                        supplier = product['supplier']['name']
                    except Exception as error:
                        supplier = ''
                    try:
                        ext_code = product['externalCode']
                    except Exception as error:
                        ext_code = ''
                    try:
                        barcode = product['barcodes'][0]['ean13']
                    except Exception as error:
                        barcode = '-'
                    try:
                        code = product['code']
                    except Exception as error:
                        code = ''
                    try:
                        category_level_1 = product['pathName'].split('/')[0]
                    except Exception as error:
                        category_level_1 = '-'
                    try:
                        category_level_2 = product['pathName'].split('/')[1]
                    except Exception as error:
                        category_level_2 = '-'
                    try:
                        article = product['article']
                    except Exception as error:
                        article = '-'
                    discount = int(position['discount'])
                    quantity = int(position['quantity'])
                    price = float(position['price']) / 100
                    final_price = price * quantity * (1 - float(discount / 100))
                    if cash_sum:
                        cash_sum = final_price
                    elif non_cash_sum:
                        non_cash_sum = final_price
                    elif qr_sum:
                        qr_sum = final_price

                    final_data.append({
                        'ext_code': ext_code,
                        'date': date,
                        'time': time,
                        'product_name': product_name,
                        'category_level1': category_level_1,
                        'category_level2': category_level_2,
                        'code': code,
                        'article': article,
                        'barcode': barcode,
                        'supplier': supplier,
                        'administrator': administrator,
                        'counterparty_name': counterparty_name,
                        'price': price,
                        'discount': discount,
                        'quantity': quantity,
                        'cash': cash_sum,
                        'non_cash': non_cash_sum,
                        'qr_sum': qr_sum,
                    })

        return final_data

    async def get_demands(self):
        retail_demand_url = 'https://online.moysklad.ru/api/remap/1.2/entity/demand'
        urls_data = self.get_urls_demand_data(retail_demand_url)
        data_from_urls = []
        final_data = []
        count = 0
        session = self.session
        for obj in urls_data:
            for el in obj:
                data = obj[el]
                positions_response = await session.get(data['positions'])
                positions_json = await positions_response.json()
                counterparty_name_response = await session.get(data['counterparty_name'])
                counterparty_name_json = await counterparty_name_response.json()

                count += 2
                if count % 45 == 0:
                    await asyncio.sleep(1)
                data_from_urls.append({
                    el: {
                        'ext_code': data['ext_code'],
                        'positions': positions_json,
                        'administrator': data['administrator'],
                        'counterparty_name': counterparty_name_json['name'],
                        'date': data['date'],
                        'time': data['time'],
                        'cashSum': data['cashSum'],
                        'noCashSum': data['noCashSum'],
                        'qrSum': data['qrSum'],
                    }
                })
        for url in data_from_urls:
            for el in url:
                data = url[el]
                for obj in data['positions']['rows']:
                    assortment = await session.get(obj['assortment']['meta']['href'])
                    assortment = await assortment.json()

                    count += 1
                    if count % 45 == 0:
                        await asyncio.sleep(1)
                    obj['product'] = assortment
        for url in data_from_urls:
            for el in url:
                data = url[el]
                for obj in data['positions']['rows']:
                    supplier_data = True
                    try:
                        supplier_url = obj['product']['supplier']['meta']['href']
                    except KeyError:
                        try:
                            supplier_response = await session.get(obj['product']['product']['meta']['href'])
                            supplier_json = await supplier_response.json()
                            supplier_url = supplier_json['supplier']['meta']['href']
                            obj['product']['pathName'] = supplier_json['pathName']
                        except:
                            supplier_url = ''
                            supplier_data = False
                    if supplier_data:
                        supplier = await session.get(supplier_url)
                        supplier = await supplier.json()

                        count += 1
                        if count % 45 == 0:
                            await asyncio.sleep(1)
                        obj['product']['supplier'] = supplier['name']
        for el in data_from_urls:
            for obj in el:
                data = el[obj]
                date = data['date']
                time = data['time']
                counterparty_name = data['counterparty_name']
                administrator = data['administrator']
                cash_sum = float(data['cashSum']) / 100
                non_cash_sum = float(data['noCashSum']) / 100
                qr_sum = float(data['qrSum']) / 100

                for position in data['positions']['rows']:
                    product = position['product']
                    product_name = product['name']
                    try:
                        supplier = product['supplier']
                    except Exception as error:
                        supplier = ''
                    try:
                        ext_code = product['externalCode']
                    except Exception as error:
                        ext_code = ''
                    try:
                        barcode = product['barcodes'][0]['ean13']
                    except Exception as error:
                        barcode = '-'
                    try:
                        code = product['code']
                    except Exception as error:
                        code = ''
                    try:
                        category_level_1 = product['pathName'].split('/')[0]
                    except Exception as error:
                        category_level_1 = '-'
                    try:
                        category_level_2 = product['pathName'].split('/')[1]
                    except Exception as error:
                        category_level_2 = '-'
                    try:
                        article = product['article']
                    except Exception as error:
                        article = '-'

                    discount = int(position['discount'])
                    quantity = int(position['quantity'])
                    price = float(position['price']) / 100
                    final_price = price * quantity * (1 - float(discount / 100))
                    if cash_sum:
                        cash_sum = final_price
                    elif non_cash_sum:
                        non_cash_sum = final_price
                    elif qr_sum:
                        qr_sum = final_price

                    final_data.append({
                        'ext_code': ext_code,
                        'date': date,
                        'time': time,
                        'product_name': product_name,
                        'category_level1': category_level_1,
                        'category_level2': category_level_2,
                        'code': code,
                        'article': article,
                        'barcode': barcode,
                        'supplier': supplier,
                        'administrator': administrator,
                        'counterparty_name': counterparty_name,
                        'price': price,
                        'discount': discount,
                        'quantity': quantity,
                        'cash': cash_sum,
                        'non_cash': non_cash_sum,
                        'qr_sum': qr_sum,
                    })

        return final_data

    def get_urls_retail_demand_data(self, url):
        urls_json = []
        rows = requests.get(url, headers=self.auth_header).json()
        count = 0
        is_next = True
        while is_next:
            for row in rows['rows']:
                urls_json.append({
                    count: {
                        'ext_code': row['externalCode'],
                        'positions': row['positions']['meta']['href'],
                        'administrator': row['owner']['meta']['href'],
                        'counterparty_name': row['agent']['meta']['href'],
                        'date': row['moment'].split(' ')[0],
                        'time': ':'.join(row['moment'].split(' ')[1].split('.')[0].split(':')[:-1]),
                        'cashSum': row['cashSum'],
                        'noCashSum': row['noCashSum'],
                        'qrSum': row['qrSum'],
                    }
                })
                count += 1
            try:
                rows = requests.get(rows['meta']['nextHref'], headers=self.auth_header).json()
            except KeyError:
                is_next = False
                print(KeyError)

        return urls_json

    def get_urls_demand_data(self, url):
        urls_json = []
        rows = requests.get(url, headers=self.auth_header).json()
        count = 0
        is_next = True
        while is_next:
            for row in rows['rows']:
                urls_json.append({
                    count: {
                        'ext_code': row['externalCode'],
                        'positions': row['positions']['meta']['href'],
                        'administrator': "Онлайн продажа",
                        'counterparty_name': row['agent']['meta']['href'],
                        'date': row['moment'].split(' ')[0],
                        'time': ':'.join(row['moment'].split(' ')[1].split('.')[0].split(':')[:-1]),
                        'cashSum': 0,
                        'noCashSum': 1,
                        'qrSum': 0,
                    }
                })
                count += 1
            try:
                rows = requests.get(rows['meta']['nextHref'], headers=self.auth_header).json()
            except KeyError:
                is_next = False
                print(KeyError)

        return urls_json

    async def get_stocks(self):
        url = 'https://online.moysklad.ru/api/remap/1.2/report/stock/all'
        data = self.get_urls_stocks(url)
        session = self.session
        count = 0
        for row in data:
            product_response = await session.get(row['product'])
            product_json = await product_response.json()
            count += 1
            if count % 45 == 0:
                await asyncio.sleep(1)
            row['product'] = product_json
            try:
                row['supplier'] = product_json['supplier']['meta']['href']
            except KeyError:
                try:
                    row['supplier'] = product_json['product']['supplier']['meta']['href']
                except:
                    row['supplier'] = None
            for el in product_json['salePrices']:
                if el['priceType']['name'] == 'Цена опт':
                    row['whole_sale_price'] = el['value']
        await asyncio.sleep(10)
        for row in data:
            if row['supplier']:
                supplier_response = await session.get(row['supplier'])
                supplier_json = await supplier_response.json()
                count += 1
                if count % 45 == 0:
                    await asyncio.sleep(1)
                row['supplier'] = supplier_json['name']

        return data

    def get_urls_stocks(self, url):
        data = requests.get(url, headers=self.auth_header).json()
        url_stocks = []
        is_next = True
        while is_next:
            for row in data['rows']:
                try:
                    article = row['article']
                except Exception as error:
                    article = None
                try:
                    category = row['folder']['pathName'].split('/')
                except Exception as error:
                    category = ['', '']
                try:
                    category_level_1 = row['folder']['pathName']
                except Exception:
                    category_level_1 = ''
                try:
                    category_level_2 = row['folder']['name']

                except Exception:
                    category_level_2 = ''
                url_stocks.append({
                    'ext_code': str(row['externalCode']),
                    'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                    'time': datetime.datetime.now().strftime('%H:%M'),
                    'code': row['code'],
                    'article': article,
                    'product_name': row['name'],
                    'unit_name': row['uom']['name'],
                    'units_aval': int(row['quantity']),
                    'units_reserve': int(row['reserve']),
                    'units_wait': int(row['inTransit']),
                    'units_stock': int(row['stock']),
                    'cost': float(row['price']) / 100,
                    'cost_sum': float(row['price']) / 100 * int(row['stock']),
                    'price': float(row['salePrice']) / 100,
                    'price_sum': float(row['salePrice']) / 100 * int(row['stock']),
                    'stock_days': float(row['stockDays']),
                    'category_level_1': category_level_1,
                    'category_level_2': category_level_2,
                    'product': row['meta']['href'],
                })
            try:
                data = requests.get(data['meta']['nextHref'], headers=self.auth_header).json()
            except KeyError:
                is_next = False
                print(KeyError)
        return url_stocks

    async def get_losses(self):
        url = 'https://online.moysklad.ru/api/remap/1.2/entity/loss'
        data = self.get_urls_losses(url)
        session = self.session
        losses_data = []
        for row in data:
            stock_response = await session.get(row['stock'])
            stock_json = await stock_response.json()
            row['stock'] = stock_json['name']
            if row['status']:
                status_response = await session.get(row['status'])
                status_json = await status_response.json()
                row['status'] = status_json['name']
            positions_response = await session.get(row['positions'])
            positions_json = await positions_response.json()
            row['positions'] = positions_json
        for row in data:
            try:
                comment = row['description']
            except Exception:
                comment = None
            for position in row['positions']['rows']:
                position_response = await session.get(position['assortment']['meta']['href'])
                position_json = await position_response.json()
                position['product'] = position_json
            for position in row['positions']['rows']:
                product_name = position['product']['name']
                supplier_exist = True
                try:
                    supplier_href = position['product']['supplier']['meta']['href']
                except KeyError:
                    try:
                        supplier_response = await session.get(position['product']['product']['meta']['href'])
                        supplier_json = await supplier_response.json()
                        supplier_href = supplier_json['supplier']['meta']['href']
                        position['product']['pathName'] = supplier_json['pathName']
                    except:
                        supplier_href = ''
                        supplier_exist = False
                if supplier_exist:
                    supplier_response = await session.get(supplier_href)
                    supplier = await supplier_response.json()
                    supplier = supplier['name']
                else:
                    supplier = ''
                try:
                    article = position['code']
                except Exception:
                    article = None
                try:
                    reason = position['reason']
                except Exception:
                    reason = None
                try:
                    path_name = position['product']['pathName']
                except Exception as error:
                    path_name = ' / '
                try:
                    category_level_1 = path_name.split('/')[0]
                except Exception as error:
                    category_level_1 = ''
                try:
                    category_level_2 = path_name.split('/')[1]
                except Exception as error:
                    category_level_2 = ''

                losses_data.append({
                    'ext_code': row['ext_code'],
                    'date': row['date'],
                    'time': row['time'],
                    'loss_id': row['loss_id'],
                    'product_name': product_name,
                    'num': int(position['quantity']),
                    'article': article,
                    'status': row['status'],
                    'comment': comment,
                    'reason': reason,
                    'price': float(position['price']) / 100,
                    'stock': row['stock'],
                    'category_level_1': category_level_1,
                    'category_level_2': category_level_2,
                    'supplier': supplier,
                })
        return losses_data

    def get_urls_losses(self, url):
        data = requests.get(url, headers=self.auth_header).json()
        url_losses = []
        is_next = True
        while is_next:
            for row in data['rows']:
                try:
                    status = row['state']['meta']['href']
                except Exception as error:
                    status = None
                url_losses.append({
                    'ext_code': row['externalCode'],
                    'date': row['moment'].split(' ')[0],
                    'time': ':'.join(row['moment'].split(' ')[1].split('.')[0].split(':')[:-1]),
                    'loss_id': row['name'],
                    'stock': row['store']['meta']['href'],
                    'status': status,
                    'positions': row['positions']['meta']['href'],

                })
            try:
                data = requests.get(data['meta']['nextHref'], headers=self.auth_header).json()
            except KeyError:
                is_next = False
                print(KeyError)
        return url_losses

    async def end_of_parsing(self):
        await self.session.close()
