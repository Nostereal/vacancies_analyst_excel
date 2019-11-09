import pandas as pd
import requests
import asyncio as aio
from aiohttp import ClientSession
from time import time
from typing import List


def init_columns():
    out_df[label_names['hh_vacancies']] = -1
    out_df[label_names['tv_vacancies']] = -1
    out_df[label_names['contracts']] = '-'
    out_df[label_names['violation']] = ''


def getIsCompanyIllegalLabel(hh_count: int, tv_count: int, has_contracts: bool) -> str:
    if hh_count > 0 and tv_count == 0:
        return 'Грубые'
    if hh_count > tv_count and has_contracts == 'Есть':
        return 'Незначительные'
  
    # otherwise
    return 'Нет'


def save_to_excel():
    global out_df
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
    returned_df.to_excel(f'./xls/async_excel_result.xlsx', startrow=0, startcol=0)


async def fetch_data(url: str, inn: int, company_name: str, session) -> (int, int, bool):
    async with session.get(url=f'{url}{inn}', params={'name' : company_name}) as response:
        try:
            data = await response.json()
        except Exception as e:
            print(e)
            save_to_excel()
            exit(-1)

        error_data = (-1, -1, False)
        if 'empty' in data.keys():
            if data['empty'] == True:
                return error_data
        if 'incorrect' in data.keys():
            if data['incorrect'] == True:
                return error_data

        has_company_contracts = data['hasCompanyContracts']
        hh_vacancies = data['hh']['total']
        tv_vacancies = data['trudVsem']['total']
        
        return hh_vacancies, tv_vacancies, has_company_contracts


async def batch_request(url: str, inns: List[int], company_names: List[str]): #  -> List[(int, int, bool)]
    tasks = []
    results = []
    async with ClientSession() as session:
        for inn, company_name in zip(inns, company_names):
            task = aio.create_task(fetch_data(url, inn, company_name, session))
            tasks.append(task)

        results = await aio.gather(*tasks)
    return results


async def handle_single_batch(url: str, batch_id: int, batch_size: int):
    t0 = time()
    curr_position = batch_id * batch_size
    curr_df = out_df[curr_position:curr_position + batch_size]
    inns, company_names = curr_df[label_names['inn']], curr_df[label_names['company_name']]

    results = await batch_request(url, inns, company_names)
    for i, triplet in enumerate(results):
        # collecting global sums about vacancies count
        global hh_sum, tv_sum
        if triplet[0] != -1:
            hh_sum += triplet[0]
            tv_sum += triplet[1]

        out_df.loc[curr_position + i, [label_names['hh_vacancies'], label_names['tv_vacancies'], label_names['contracts']]] = triplet

        has_contrs = triplet[2]
        out_df.loc[curr_position + i, label_names['contracts']] = 'Есть' if has_contrs else 'Нет'
        
    print(f'Batch #{batch_id + 1} elapsed time by batch in main func: {round(time() - t0, 3)} s')


async def main(url: str, batch_size: int):
    n_full_batches = n_companies // batch_size
    n_remaining_requests = n_companies % batch_size

    for batch in range(n_full_batches):
        await handle_single_batch(url, batch, batch_size)

    # last batch for remaining requests
    await handle_single_batch(url, n_full_batches, n_remaining_requests)


if __name__ == '__main__':
    start_time = time()
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
    df = pd.read_excel('./xls/Список_заказчиков.xlsx', sheet_name=0)

    out_df = df[[label_names['index'], label_names['company_name'], label_names['inn']]].copy()
    del df

    n_companies = out_df.shape[0]
    init_columns()
    # run async coroutine that will fetch responses simultaneous by batch
    print('...Start sending requests...')
    aio.run(main(url, batch_size=30))
    # fill_columns_from_api()
    for i in range(n_companies):
        out_df.loc[i, label_names['violation']] = getIsCompanyIllegalLabel(out_df.loc[i, label_names['hh_vacancies']], out_df.loc[i, label_names['tv_vacancies']], out_df.loc[i, label_names['contracts']])

    print(f'Вакансий на hh.ru: {hh_sum}\nВакансий на trudvsem: {tv_sum}')
    save_to_excel()
    print(f'Total time: {round(time() - start_time, 3)} s')