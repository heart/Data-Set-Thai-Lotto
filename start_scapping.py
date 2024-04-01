import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List
import pandas as pd
import os
from datetime import datetime


@dataclass
class ArchivePage:
    archiveList: List[str]
    nextPageUrl: str


def getArchivePage(url):
    print(f"Page = {url}")
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        nextPageUrl = ""
        lotto = []
        for link in soup.find_all('a', class_='pagination__item--next'):
            nextPageUrl = link.get('href')
            break

        divContent = soup.find(
            'div', class_=['box-cell', 'box-cell--lotto', 'content'])

        for link in divContent.find_all('a'):
            lottoUrl = link.get('href')
            if "/lotto/check/" in lottoUrl:
                lotto.append(lottoUrl)

        return ArchivePage(archiveList=lotto, nextPageUrl=nextPageUrl)
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


def getDate(url):
    d = url.split("/")

    while ("" in d):
        d.remove("")
    dateStr = d[len(d)-1]
    date = dateStr[0:2]
    month = dateStr[2:4]
    year = str(int(dateStr[4:8])-543)
    return f"{year}-{month}-{date}"


def scappingLotto(url):
    if url == 'https://news.sanook.com/lotto/check/ผลสลากกินแบ่งรัฐบาลงวดประจำวันที่1สิงหาคม2552/':
        # แก้ไข เฉพาะลิงค์นี้ เพราะหน้าเว็ปสนุก ให้ URL  มาไม่ถูกต้อง
        url = 'https://news.sanook.com/lotto/check/01082552/'

    response = requests.get(url)
    date = getDate(url)
    print(f'{date} = {url}')

    row = {
        'date': date,
        'prize_1st': '',
        'prize_pre_3digit': [],
        'prize_sub_3digits': [],
        'prize_2digits': [],
        'nearby_1st': [],
        'prize_2nd': [],
        'prize_3rd': [],
        'prize_4th': [],
        'prize_5th': []
    }

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        columns = soup.find_all('div', class_='lottocheck__column')
        if columns:
            if len(columns) == 4:
                i = 0
                prefix = []
                subfix = []
                first = ''
                last2digit = ''
                for col in columns:
                    for num in col.find_all('strong'):
                        if i == 0:
                            first = num.text
                        elif i == 1:
                            prefix.append(num.text)
                        elif i == 2:
                            subfix.append(num.text)
                        elif i == 3:
                            last2digit = num.text
                    i = i+1
                row['prize_1st'] = first
                row['prize_pre_3digit'] = prefix
                row['prize_sub_3digits'] = subfix
                row['prize_2digits'] = last2digit
            else:
                i = 0
                prefix = []
                subfix = []
                first = ''
                last2digit = ''
                for col in columns:
                    for num in col.find_all('strong'):
                        if i == 0:
                            first = num.text
                        elif i == 1:
                            subfix.append(num.text)
                        elif i == 2:
                            last2digit = num.text
                    i = i+1
                row['prize_1st'] = first
                row['prize_pre_3digit'] = prefix
                row['prize_sub_3digits'] = subfix
                row['prize_2digits'] = last2digit

        nearby = []
        div = soup.find('div', class_='lottocheck__sec--nearby')
        if div:
            for ele in div.find_all('strong', class_="lotto__number"):
                nearby.append(ele.text)
            row['nearby_1st'] = nearby

        i = 0
        sections = soup.find_all('div', class_='lottocheck__sec')
        if sections:
            for section in sections:
                divs = section.find_all('div', class_='lottocheck__box-item')

                nums = []
                if i == 0:
                    i = i+1
                    continue

                for div in divs:
                    for span in div.find_all('span', class_='lotto__number'):
                        nums.append(span.text)
                if i == 1:
                    row['prize_2nd'] = nums
                elif i == 2:
                    row['prize_3rd'] = nums
                elif i == 3:
                    row['prize_4th'] = nums
                elif i == 4:
                    row['prize_5th'] = nums
                i = i+1
            return row
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


columns = ['date', 'prize_1st', 'prize_pre_3digit',
           'prize_sub_3digits', 'prize_2digits', 'nearby_1st', 'prize_2nd', 'prize_3rd', 'prize_4th', 'prize_5th']
# dtypes = {
#     'date': datetime,
#     'prize_1st': str,
#     'prize_pre_3digit': object,
#     'prize_sub_3digits': object,
#     'prize_2digits': object,
#     'nearby_1st': object,
#     'prize_2nd': object,
#     'prize_3rd': object,
#     'prize_4th': object,
#     'prize_5th': object
# }

df = pd.DataFrame(columns=columns)

archive_url = "https://news.sanook.com/lotto/archive/"
header = True

if os.path.exists('lotto.csv'):
    os.remove('lotto.csv')

if os.path.exists('lotto.parquet'):
    os.remove('lotto.parquet')

while True:
    archive = getArchivePage(archive_url)

    for page in archive.archiveList:
        new_row = scappingLotto(page)
        newDf = pd.DataFrame([new_row])
        df = pd.concat([df, newDf], ignore_index=True)
        newDf.to_csv('lotto.csv', mode='a', index=False, header=header)
        header = False

    if archive.nextPageUrl == "":
        df.to_parquet('lotto.parquet', index=False)
        break
    else:
        archive_url = archive.nextPageUrl
