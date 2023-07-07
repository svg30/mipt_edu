import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, insert, ForeignKey
from sqlalchemy import URL
from sqlalchemy import select
from typing import Optional
import keyring
import asyncio
import time
from aiohttp import ClientSession
import logging


PSQL_USER = 'edu_mipt'
key_words = ['middle', 'Python', ]
user_agent = {'User-agent': 'Mozilla/5.0'}

logging.basicConfig(filename='async.log', level=logging.DEBUG)

class Base(DeclarativeBase):
    pass


class Vacancy1(Base):
    __tablename__ = 'vacancy1'
    id: Mapped[int] = mapped_column(primary_key=True)
    company_name: Mapped[Optional[str]]
    position: Mapped[Optional[str]]
    job_description: Mapped[Optional[str]]
    key_skills: Mapped[Optional[str]]
    salary: Mapped[Optional[str]]
    vac_id: Mapped[Optional[str]]

    def __repr__(self):
        return f"{self.company_name} {self.position}"


class Vacancy2(Base):
    __tablename__ = 'vacancy2'
    id: Mapped[int] = mapped_column(primary_key=True)
    company_name: Mapped[Optional[str]]
    position: Mapped[Optional[str]]
    job_description: Mapped[Optional[str]]
    key_skills: Mapped[Optional[str]]
    salary: Mapped[Optional[str]]
    vac_id: Mapped[Optional[str]]

    def __repr__(self):
        return f"{self.company_name} {self.position}"


def connect_to_db():
    passwd = keyring.get_password('edu_mipt', PSQL_USER)
    if not passwd:
        passwd = input('Enter password')
        keyring.set_password('edu_mipt', PSQL_USER, passwd)
    url_object = URL.create(
        "postgresql+psycopg2",
        username=PSQL_USER,
        password=passwd,
        host="10.146.101.12",
        database="edu",
    )
    return create_engine(url_object, echo=True)


def save_to_db(vacancies):
    engine = connect_to_db()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.execute(insert(Vacancy1), vacancies)
        session.commit()

def save_to_db1(vacancies):
    engine = connect_to_db()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.execute(insert(Vacancy2), vacancies)
        session.commit()


def get_vacancy_list_with_requests():
    #key_words = ['middle', 'Python', ]
    hh_link = f"https://hh.ru/search/vacancy?no_magic=true&L_save_area=true&text={'+'.join(key_words)}&search_field=name&excluded_text=&area=1&salary=&currency_code=RUR&experience=doesNotMatter&order_by=relevance&search_period=0&items_on_page=100"
    #user_agent = {'User-agent': 'Mozilla/5.0'}
    result = requests.get(hh_link, headers=user_agent)
    vacancy_id_list = []
    i=0
    while result.status_code == 200:
        soup = BeautifulSoup(result.content.decode(), 'lxml')
        names = soup.find_all('a', attrs={'data-qa':'serp-item__title'})
        if len(names) == 0:
            break
        for name in names:
            vacancy_id_list.append(name['href'].split('vacancy/')[1].split('?')[0])
        i += 1
        result = requests.get(hh_link+f"&page={i}", headers=user_agent)
    return vacancy_id_list


def get_vacancy_info_sync(vac_id):
    #user_agent = {'User-agent': 'Mozilla/5.0'}
    result = requests.get(f"https://hh.ru/vacancy/{vac_id}", headers=user_agent)
    soup = BeautifulSoup(result.content.decode(), 'lxml')
    company_name = y.text if (y:=soup.find(attrs={'data-qa': 'vacancy-company-name'})) else ""
    # Выбираем название позиции
    position = y.text if (y:=soup.find(attrs={'data-qa': 'vacancy-title'})) else ""
    # Выбираем название позиции
    salary = y.text if (y:=soup.find(attrs={'data-qa': 'vacancy-salary'})) else ""
    # Выбираем описание вакансии
    job_description = str(y) if (y := soup.find( attrs={'data-qa': 'vacancy-description'})) else ""
    # Выбираем ключевые навыки

    def custom_selector(tag):
        # Return "span" tags with a class name of "target_span"
        return tag.has_attr("data-qa") and "skills-element" in tag.get("data-qa")
    names = soup.find_all(custom_selector)
    key_skills = ', '.join([i.text for i in names])
    return {"company_name": company_name, "salary": salary, 
                        "position": position,
                        "job_description": job_description, 
                        "key_skills": key_skills, "vac_id": vac_id}


def fill_vacancies_sync():
    vacancies = []
    for vac_id in get_vacancy_list_with_requests():
        vacancies.append(get_vacancy_info_sync(vac_id))
    save_to_db(vacancies)


async def get_vacancy_info_async(id, session):
    logging.debug(f'start getting vacancy https://api.hh.ru/vacancies/{id}')
    async with session.get(f'/vacancies/{id}') as response:
        vac = await response.json()
        logging.debug(f'vacancy ->\n{vac}')

        def get_salary(salary):
            if not salary or not salary['from']:
                return ''
            return str(salary['from'])+' '+str(salary['to']) if salary['to'] else str(salary['from'])
        
        return {"company_name": vac['employer']['name'], 
                                        "salary": get_salary(vac['salary']), 
                                        "position": vac['name'],
                                        "job_description": vac['description'], 
                                        "key_skills": ', '.join([i["name"] for i in vac['key_skills']]), 
                                        "vac_id": vac['id']}


async def fill_vacancies_acync():
    async with ClientSession('https://api.hh.ru/') as session:
        url = f'/vacancies?text={" ".join(key_words)}&per_page=20'
        page=0
        while True:
            logging.debug(f'start getting vacancies {url}')
            async with session.get(url=url) as response:
                vacs_json = await response.json()
                logging.debug(f'vacancies ->\n{vacs_json}')
                vac_tasks = []
                for item in vacs_json["items"]:
                    vac_tasks.append(asyncio.create_task(get_vacancy_info_async(item["id"], session)))
                page += 1
                if int(vacs_json['pages'])>page:
                    url = f'/vacancies?text={" ".join(key_words)}&per_page=20&page={page}'
                else:
                    break
                results = await asyncio.gather(*vac_tasks)
                save_to_db1(results)
    
    
def main():
    # Получаем вакансии синхронным способом
    fill_vacancies_sync()
    
    # Получаем вакансии асинхронно
    asyncio.run(fill_vacancies_acync())
    
    
    
if __name__ == "__main__":
    main()
