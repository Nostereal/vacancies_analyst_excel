import pandas as pd
import requests
import json
import asyncio
from time import time


def init_columns():
    out_df[label_names['hh_vacancies']] = -1
    out_df[label_names['tv_vacancies']] = -1
    out_df[label_names['contracts']] = '-'
    out_df[label_names['violation']] = ''


def fill_columns_from_api():
    print(out_df.shape[0])
    global hh_sum, tv_sum

    for ind, inn in enumerate(out_df[label_names['inn']]):
        t0 = time()
        try:
            json_response = json.loads(requests.get(url=f'{url}{inn}', params={'name': out_df.loc[ind, label_names['company_name']]}).text)
        except Exception as e:
            print(e)
            save_to_excel()
            # loop.close()
            exit(-1)

        if 'empty' in json_response.keys():
            if json_response['empty'] == True:
                continue
        if 'incorrect' in json_response.keys():
            if json_response['incorrect'] == True:
                continue
        
        curr_hh_count = json_response['hh']['total']
        curr_trudvsem_count = json_response['trudVsem']['total']

        out_df.loc[ind, label_names['hh_vacancies']] = curr_hh_count
        out_df.loc[ind, label_names['tv_vacancies']] = curr_trudvsem_count
        out_df.loc[ind, label_names['contracts']] = 'Есть' if json_response['hasCompanyContracts'] else 'Нет'

        hh_sum += curr_hh_count
        tv_sum += curr_trudvsem_count
        print(f'{ind}...{round(time() - t0, 2)}')


def getIsCompanyIllegalLabel(hh_count, tv_count, has_contracts):
    if hh_count > 0 and tv_count == 0:
        return 'Грубые'
    if hh_count > tv_count and has_contracts == 'Есть':
        return 'Незначительные'
  
    # otherwise
    return 'Нет'


def save_to_excel():
    # need to increment version by yourself
    version = 10
    returned_df = out_df[
        [
            label_names['index'], 
            label_names['company_name'], 
            label_names['inn'], 
            label_names['violation'], 
            label_names['hh_vacancies'], 
            label_names['tv_vacancies'], 
            label_names['contracts']
        ]
    ]
    del out_df
    returned_df.set_index(label_names['index'], inplace=True)
    returned_df.to_excel(f'./xls/new_api_big_batch_v{version}.xlsx', start_row=0, start_col=0)


def get_batch_of_data_to_tests(df, data_size):
    out_df = df[:data_size]
    return [(inn, comp_name) for inn, comp_name in zip(out_df['Наименование организации'], out_df['ИНН'])]


if __name__ == '__main__':
    label_names = {
            'index' : '№',
            'company_name' : 'Наименование организации',
            'inn' : 'ИНН',
            'violation' : 'Нарушения',
            'hh_vacancies' : 'Вакансий на HeadHunter',
            'tv_vacancies' : 'Вакансий на TrudVsem',
            'contracts' : 'Гос. контракты'
    }

    url = 'https://polar-island-74903.herokuapp.com/inn/'
    hh_sum, tv_sum = 0, 0 
    df = pd.read_excel('./xls/Список_организаций_2019-10-19_23.40.44.xlsx', sheet_name=0)

    out_df = df[[label_names['index'], label_names['company_name'], label_names['inn']]].copy()
    del df

    init_columns()

    # loop = asyncio.get_event_loop()
    # smth = asyncio.create_task() # pass an async def in this method 
    # loop.run_until_complete(smth)

    fill_columns_from_api()
    for i in range(out_df.shape[0]):
        out_df.loc[i, label_names['violation']] = getIsCompanyIllegalLabel(out_df.loc[i, label_names['hh_vacancies']], out_df.loc[i, label_names['tv_vacancies']], out_df.loc[i, label_names['contracts']])

    print(f'Вакансий на hh.ru: {hh_sum}\nВакансий на trudvsem: {tv_sum}')
    save_to_excel()