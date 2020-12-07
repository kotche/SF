import requests
from bs4 import BeautifulSoup
import re
import json
import datetime as dt
import pandas as pd
from joblib import Parallel, delayed
from tqdm.notebook import tqdm
import time

# https://www.youtube.com/watch?v=zKuBDil5dlw&t=1773s
# Обновить страницу - F12 - network(сеть) - headers (первая строчка,названия из элементо справочника),"accept": "*/*"( "*/*" хватает)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
    'accept': '*/*'}

COLUMNS = ['bodyType', 'brand', 'color', 'fuelType', 'image', 'modelDate', 'name', 'numberOfDoors', 'productionDate',
           'vehicleConfiguration',
           'vehicleTransmission', 'priceCurrency', 'engineDisplacement', 'enginePower', 'description', 'car_url',
           'mileage', 'sell_id',
           'model_name', 'Владельцы', 'Владение', 'ПТС', 'Привод', 'Руль', 'Состояние', 'Таможня', 'parsing_unixtime',
           'complectation_dict', 'equipment_dict', 'equipment_groups', 'super_gen', 'vendor', 'model_info',
           'plus_minus', 'damages', 'price_info', 'price']

BRENDS = ['audi', 'bmw', 'cadillac', 'chery', 'chevrolet', 'chrysler',
          'citroen', 'daewoo', 'dodge', 'ford', 'geely', 'honda', 'hyundai',
          'infiniti', 'jaguar', 'jeep', 'kia', 'lexus', 'mazda', 'mini',
          'mitsubishi', 'nissan', 'opel', 'peugeot', 'porsche', 'renault',
          'skoda', 'subaru', 'suzuki', 'toyota', 'volkswagen', 'volvo',
          'great_wall', 'land_rover', 'mercedes', 'ssang_yong']

START_YEAR = 1960  # 1960
END_YEAR = 2021

START = dt.datetime.now()


# получить html код странички
def get_page(url):
    response = requests.get(headers=HEADERS, url=url)
    page = BeautifulSoup(response.content, 'html.parser')
    return page


# получить количество страниц
def get_count_page(page):
    # count_page = page.find('span',
    #                        class_='ControlGroup ControlGroup_responsive_no ControlGroup_size_s ListingPagination-module__pages')

    count_page = page.find('div',
                           class_='ListingCarsPagination-module__container')

    page_lst = count_page.find_all('span', class_="Button__text")
    last_line = page_lst[len(page_lst) - 3]
    return int(last_line.text)


# получить все тачки по бренду
def get_cars_info(brend):
    print(f'{brend} : обработка началась')

    for year in range(START_YEAR, END_YEAR):

        year_str = str(year)
        url = f'https://auto.ru/moskva/cars/{brend}/{year_str}-year/all/'

        page = get_page(url)
        try:
            count_page = get_count_page(page)
        except:
            continue

        cars_info = []

        try:
            print(f'{brend} : год {year} , всего страниц: {count_page}')
        except:
            continue

        for i in range(1, count_page + 1):
            page_i = get_page(url + f'?output_type=list&page={i}')
            car_lst = page_i.find_all('a', class_='Link ListingItemTitle-module__link')

            for car in car_lst:
                url_car = car.get('href')
                car_info = get_car_info(url_car)

                cars_info.append(car_info)

            # time.sleep(1)
            print(f'{brend} : год {year} , текущая страница: {i}, записей в списке: {len(cars_info)}')

        len_file = add_file_info(brend, cars_info)
        print(f'{brend} : записей в файле {len_file}')
        print(f'{brend} : прошло времени: {dt.datetime.now() - START}')

    print(f'{brend} : готово!')


# Получить данные из карточки
def get_card_info(cardInfo, class_m):
    val = cardInfo.find('li', class_=class_m)
    return val.find_all('span', class_='CardInfoRow__cell')[1].string


# получить данные конкретной тачки
def get_car_info(url):
    car_info = {}
    page = get_page(url)

    set_meta = page.find_all('meta')

    for el in set_meta:
        try:
            feature = el['itemprop']
            if feature in COLUMNS:
                car_info[feature] = el['content']
        except:
            continue

    car_info['car_url'] = url

    script_init = page.find('script', id='initial-state')
    try:
        car_info['mileage'] = re.findall('"mileage":(\d+)', script_init.string)[0]
    except:
        car_info['mileage'] = None

    try:
        car_info['sell_id'] = int(re.findall('"sale_id":"(\w+)"', script_init.string)[0])
    except:
        car_info['sell_id'] = None

    try:
        model_info = script_init.string[re.search(r'"model_info":{', script_init.string).regs[0][1]:]
        car_info['model_name'] = re.findall('"name":"(\w+)"', model_info)[0]
    except:
        car_info['model_name'] = None

    cardInfo = page.find('ul', class_='CardInfo')

    try:
        car_info['Владельцы'] = int(re.findall('\d', get_card_info(cardInfo, 'CardInfoRow_ownersCount'))[0])
    except:
        car_info['Владельцы'] = None

    try:
        car_info['Владение'] = get_card_info(cardInfo, 'CardInfoRow_owningTime')
    except:
        car_info['Владение'] = None

    try:
        car_info['ПТС'] = get_card_info(cardInfo, 'CardInfoRow_pts')
    except:
        car_info['ПТС'] = None

    try:
        car_info['Привод'] = get_card_info(cardInfo, 'CardInfoRow_drive')
    except:
        car_info['Привод'] = None

    try:
        car_info['Руль'] = get_card_info(cardInfo, 'CardInfoRow_wheel')
    except:
        car_info['Руль'] = None

    try:
        car_info['Состояние'] = get_card_info(cardInfo, 'CardInfoRow_state')
    except:
        car_info['Состояние'] = None

    try:
        car_info['Таможня'] = get_card_info(cardInfo, 'CardInfoRow_customs')
    except:
        car_info['Таможня'] = None

    try:
        car_info['parsing_unixtime'] = dt.datetime.now().timestamp().__round__()
    except:
        car_info['parsing_unixtime'] = None

    ######################################################################## parsing info json
    info = page.find('script', id='initial-state')
    try:
        info_json = json.loads(info.string)
        vehicle_info = info_json['card']['vehicle_info']
    except:
        pass

    try:
        car_info['complectation_dict'] = vehicle_info['complectation']
    except:
        car_info['complectation_dict'] = None

    try:
        car_info['equipment_dict'] = vehicle_info['equipment']
    except:
        car_info['equipment_dict'] = None

    try:
        car_info['equipment_groups'] = vehicle_info['equipmentGroups']
    except:
        car_info['equipment_groups'] = None

    try:
        car_info['super_gen'] = vehicle_info['super_gen']
    except:
        car_info['super_gen'] = None

    try:
        car_info['vendor'] = vehicle_info['vendor']
    except:
        car_info['vendor'] = None

    try:
        car_info['model_info'] = vehicle_info['model_info']
    except:
        car_info['model_info'] = None

    try:
        car_info['plus_minus'] = info_json['reviewsFeatures']['data']['features']
    except:
        car_info['plus_minus'] = None

    try:
        car_info['damages'] = info_json['card']['state']['damages']
    except:
        car_info['damages'] = None

    try:
        car_info['price_info'] = info_json['card']['price_info']
    except:
        car_info['price_info'] = None

    try:
        car_info['price'] = info_json['card']['price_info']['price']
    except:
        car_info['price'] = None

    ######################################################################## parsing info
    ########################################################################dop info
    # try:
    #     title = page.find_all('div', class_='CardBenefits__item-title')
    #     text = page.find_all('div', class_='CardBenefits__item-text')
    #     dop_info = '{'
    #
    #     for i in range(0, len(title)):
    #         dop_info += f'"{title[i].text}": "{text[i].text}"' + ','
    #
    #     dop_info = dop_info[:len(dop_info) - 1]
    #     dop_info = dop_info + '}'
    #     car_info['dop_info'] = dop_info.replace(u'\xa0', ' ') #убрать символ nbsp (неразрывный пробел)
    # except:
    #     car_info['dop_info'] = None

    car_info_lst = []
    for col in COLUMNS:
        try:
            car_info_lst.append(car_info[col])
        except:
            car_info_lst.append(None)

    return car_info_lst


# запись файла
def write_file(brend, cars_info):
    pd.DataFrame(cars_info, columns=COLUMNS).to_csv(f'{brend}.csv', index=False)


# общая функция получения данных и записи файла
def add_file_info(brend, cars_info):
    try:
        data_temp = pd.read_csv(f'{brend}.csv')
        data = pd.concat([data_temp, pd.DataFrame(cars_info, columns=COLUMNS)])
        data.to_csv(f'{brend}.csv', index=False)
    except:
        pd.DataFrame(cars_info, columns=COLUMNS).to_csv(f'{brend}.csv', index=False)
        print(f'{brend}.csv создан!')

    return len(pd.read_csv(f'{brend}.csv'))


# объединить все файлы
def concat_all_files():
    data = pd.DataFrame()
    for brend in BRENDS:
        try:
            data_temp = pd.read_csv(f'{brend}.csv')
        except:
            continue

        data = pd.concat([data, data_temp])

    data.to_csv('parsing_all_moscow_auto_ru_24_11_20.csv', index=False)


# Parallel(n_jobs=6)(delayed(get_cars_info)(brend) for brend in tqdm(BRENDS))  # распараллельвание процессов.

for brend in BRENDS:
    get_cars_info(brend)

end = dt.datetime.now()
print(f'ОБЩЕЕ ВРЕМЯ : {end - START}')

# concat_all_files()
