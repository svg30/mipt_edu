from datetime import datetime, timedelta
import itertools
import json
import sqlite3
import time
import zipfile
from multiprocessing import Pool
import asyncio
import logging

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, insert
from sqlalchemy import select
from typing import Optional
from typing import List
import pandas as pd


from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from aiohttp import ClientSession

default_args = {
    'owner': 'svg',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

key_words = ['middle', 'Python', ]
user_agent = {'User-agent': 'Mozilla/5.0'}

logging.basicConfig(filename='async.log', level=logging.DEBUG)

Base = declarative_base()


class Egrul(Base):
    __tablename__ = 'telecom_companies'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    full_name = Column(String)
    ogrn = Column(String)
    inn = Column(String)
    kpp = Column(String)
    okved = Column(String)

    def __repr__(self):
        return f"{self.name} {self.ogrn} {self.inn} {self.okved}"


class Vacancy2(Base):
    __tablename__ = 'vacancy2'
    id = Column(Integer, primary_key=True)
    company_name = Column(String, nullable=True)
    position = Column(String, nullable=True)
    job_description = Column(String, nullable=True)
    key_skills = Column(String, nullable=True)
    salary = Column(String, nullable=True)
    vac_id = Column(String, nullable=True)

    def __repr__(self):
        return f"{self.company_name} {self.position}"


def save_to_db(data):
    ps_hook = PostgresHook(postgres_conn_id='hh_db')
    engine = ps_hook.get_sqlalchemy_engine()
    with Session(engine) as session:
        session.execute(insert(Egrul), data)
        session.commit()

def get_file_data(filename):
    ul_list = []
    with zipfile.ZipFile('/home/svg/egrul.json.zip') as f:
        with f.open(filename) as f2:
            data = json.loads(f2.read())
            for d in data:
                try:
                    if d['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'] == '61' or \
                            d['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'].startswith('61.'):
                        d1 = {"ogrn": d['ogrn'],
                              'inn': d['inn'],
                              'kpp': d['kpp'],
                              'name': d['name'],
                              'full_name': d['full_name'],
                              'okved': d['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']}
                        ul_list.append(d1)
                except KeyError:
                    pass
            return ul_list




async def get_vacancy_info_async(id, session):
    logger = logging.getLogger(__name__)
    logger.info(f'start getting vacancy https://api.hh.ru/vacancies/{id}')
    async with session.get(f'/vacancies/{id}') as response:
        vac = await response.json()
        logger.info(f'vacancy ->\n{vac}')

        def get_salary(salary):
            if not salary or not salary['from']:
                return ''
            return str(salary['from'])+' '+str(salary['to']) if salary['to'] else str(salary['from'])
        
        try:
            ret_val = {"company_name": vac['employer']['name'], 
                                        "salary": get_salary(vac['salary']), 
                                        "position": vac['name'],
                                        "job_description": vac['description'], 
                                        "key_skills": ', '.join([i["name"] for i in vac['key_skills']]), 
                                        "vac_id": vac['id']}
            return ret_val
        except Exception:
            pass

async def fill_vacancies_acync():
    logger = logging.getLogger(__name__)
    async with ClientSession('https://api.hh.ru/') as session:
        url = f'/vacancies?text={" ".join(key_words)}&per_page=20'
        page=0
        while True:
            logger.info(f'******* start getting vacancies {url}')
            async with session.get(url=url) as response:
                vacs_json = await response.json()
                logger.info(f'vacancies ->\n{vacs_json}')
                vac_tasks = []
                for item in vacs_json["items"]:
                    vac_tasks.append(asyncio.create_task(get_vacancy_info_async(item["id"], session)))
                page += 1
                if int(vacs_json['pages'])>page:
                    url = f'/vacancies?text={" ".join(key_words)}&per_page=20&page={page}'
                else:
                    break
                results = await asyncio.gather(*vac_tasks)
                logger.info(results)
                save_to_db1(results)
                time.sleep(5)

def save_to_db1(vacancies):
    engine = PostgresHook(postgres_conn_id='hh_db').get_sqlalchemy_engine()
    with Session(engine) as session:
        session.execute(insert(Vacancy2), vacancies)
        session.commit()


def fill_vacancies(**kwargs):
    asyncio.run(fill_vacancies_acync())


def parse_and_save_egrul(**kwargs):
    data = []
    with zipfile.ZipFile('/home/svg/egrul.json.zip') as f:
        i=0
        file_list = [i for i in f.filelist]
        #file_list = file_list[:100]
        with Pool(10) as pool:
            t = pool.map(get_file_data, file_list)
    t = list(itertools.chain(*t))
    save_to_db(t)


def get_telecom_vacancies_skills(**kwargs):
    logger = logging.getLogger(__name__)
    companies = []
    skills={}

    def clear_name(name):
        return name.replace('ООО ',' ').replace('"','').replace('ПАО ',' ').strip().lower()
    
    with Session(PostgresHook(postgres_conn_id='hh_db').get_sqlalchemy_engine()) as session:
        results = session.execute(select(Egrul))
        for row in results:
            companies.append(clear_name(row[0].name))

        results = session.execute(select(Vacancy2))
        for row in results:
            if clear_name(row[0].company_name) in companies:
                for skill in row[0].key_skills.split(','):
                    skill = skill.strip()
                    if skill not in skills.keys():
                        skills[skill]=1
                    else:
                        skills[skill] += 1
        sorted_skills = dict(sorted(skills.items(), key=lambda item: item[1], reverse=True)[:5])
        print(sorted_skills)
        logger.info("!!!!!!!!!!!!!!!!!!!!", sorted_skills)
        

with DAG(
    dag_id='Import_hh_DAG',
    default_args=default_args,
    description='DAG for import hh vacancies',
    start_date=datetime(2023, 8, 9, 8),
    schedule_interval='@daily'
) as dag:
    download_task = BashOperator(
        task_id='download_file',
        bash_command="wget https://ofdata.ru/open-data/download/egrul.json.zip -N -O /home/svg/egrul.json.zip"
  
    )

    parse_file_task = PythonOperator(
        task_id='parse_and_save_egrul',
        python_callable=parse_and_save_egrul,
    )

    fill_vacancies_task = PythonOperator(
        task_id='fill_vacancies',
        python_callable=fill_vacancies,
    )
    
    get_company_skills = PythonOperator(
        task_id='company_skills',
        python_callable=get_telecom_vacancies_skills,
    )
    

    download_task >> parse_file_task >> fill_vacancies_task >> get_company_skills
