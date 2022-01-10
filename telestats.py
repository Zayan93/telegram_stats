from pyrogram import Client
from telethon import TelegramClient
import asyncio
import requests
import os
from telethon.tl.functions.messages import (GetHistoryRequest)
from telethon.tl import functions, types
from datetime import datetime
import pandas as pd
import re
import time
import dataframe_image as dfi
from termcolor import colored
from dotenv import load_dotenv

load_dotenv()

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')

my_channel_id = (
    -1001435566697,
    -1001199979298,
    -1001075101206,
    -1001340914456,
    -1001197210433,
    -1001351339368,
    -1001292964247,
    -1001414133865,
    -1001290170364,
    -1001300011741,
    -1001396224644,
    -1001533623424,
    -1001203560567,
    # -1001347991731,
    -1001160968703,
    -1001590375056,
    -1001478869165,
    -1001441563903,
    -1001445446933,
    -1001179307709,
    -1001411876344,
    -1001401655087
)  # все каналы
other_channels = (
    -1001340914456,
    -1001197210433,
    -1001396224644,
    -1001203560567,
    -1001441563903,
    -1001179307709
    # -1001347991731
)  # Каналы поиск идет по снежинке
other_channels_1 = (
    -1001351339368,
    -1001292964247,
    -1001414133865,
    -1001533623424,
    -1001435566697,
    -1001160968703,
    -1001590375056,
    -1001478869165,
    -1001401655087,
    -1001411876344
)  # Каналы поиск идет по доллару
other_channels_2 = (
    -1001199979298,
    -1001075101206,
    -1001290170364,
    -1001445446933
)  # Каналы поиск идет по скобкам
other_channels_3 = (-1001300011741)  # Каналы поиск идет по слову тикер

authors = {
    -1001199979298: 'bitkogan',
    -1001075101206: 'РынкиДеньгиВласть | РДВ',
    -1001197210433: 'Сигналы РЦБ',
    -1001340914456: 'Rezan Invest',
    -1001351339368: 'ВТБ Мои Инвестиции',
    -1001292964247: 'Full-Time Trading',
    -1001414133865: 'Диванный инвестор',
    -1001290170364: 'Инвестэкономика',
    -1001300011741: 'Finam Alert',
    -1001396224644: 'ДИВИДЕНДЫ INVESTMINT',
    -1001533623424: 'Чуйка Инвестмент Груп',
    -1001203560567: 'MarketTwits',
    -1001376378526: 'Klepinvest',
    # -1001347991731: 'Stock News',
    -1001435566697: 'Сашкины финансы',
    -1001160968703: 'ETP Trading',
    -1001590375056: 'Long Trade',
    -1001478869165: 'RocketToTheMoon',
    -1001441563903: 'Smart.Lab',
    -1001445446933: 'Дача в Дубае',
    -1001179307709: 'Never Short Tesla',
    -1001411876344: 'Pharmacolog - инвестиции',
    -1001401655087: 'TRADE SYSTEM'
}


client = TelegramClient('myGrab', api_id, api_hash)
client.start()


def send_message():
    with Client("my_account", api_id, api_hash) as app:
        app.send_photo(-1001545660559, photo=open('telestats.png', 'rb'))


async def get_telestats(dat):

    dict = []

    for ch in my_channel_id:

        channel = await client.get_entity(ch)

        if ch in other_channels:
            pattern = r"\#[A-Z]+"
        elif ch in other_channels_1:
            pattern = r"\$[A-Z]+"
        elif ch in other_channels_2:
            pattern = r"\((.*?)\b[A-Z]+\)"
        else:
            pattern = r"\тикер [A-Z]+"

        ye = int(format(dat, "%Y"))
        mon = int(format(dat, "%m"))
        da = int(format(dat, "%d"))
        date_of_post = datetime(ye, mon, da)
        messages = await client.get_messages(channel, limit=None, reverse=True, offset_date=date_of_post)  #pass your own args

        for i in messages:
            try:
                string = i.text
                match = re.findall(pattern, string)
                for i in match:
                    if i == None:
                        pass
                    c = re.findall(r"\b[A-Z]+", i)
                    try:
                        new = {'Тикер': c[0], 'Дата': date_of_post.strftime("%m/%d/%Y"), 'Каналы': authors[ch]}
                        dict.append(new)
                    except IndexError:
                        pass
            except TypeError:
                pass

    df = pd.DataFrame(dict)
    count_mentions_filter = pd.DataFrame(df.groupby(['Тикер', 'Дата']).size().reset_index(name='Кол-во обсуждений'))
    ordering_filter = pd.DataFrame(count_mentions_filter.sort_values(by=['Кол-во обсуждений'], ascending=False, ignore_index=True)[:20])
    mentions = ordering_filter.style.set_caption(f'<b> Топ-20 обсуждаемых компаний в ТЕЛЕГРАММ-КАНАЛАХ </b>').background_gradient(cmap='YlOrRd')

    with pd.ExcelWriter('data.xlsx', mode='a') as writer:
        df.to_excel(writer, sheet_name='channel')
        mentions.to_excel(writer, sheet_name='mentions')

    dfi.export(mentions, 'telestats.png')


def main():

    while True:
        ts = datetime.now()
        ft = datetime(
            hour=8,
            year=int(format(ts, "%Y")),
            month=int(format(ts, "%m")),
            day=int(format(ts, "%d"))
        )
        try:
            if ts > ft:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(get_telestats(ts))
                send_message()
                time.sleep(60 * 2)
            else:
                pass

        except Exception as e:
            print(f"Бот упал с ошибкой: {e}")
            time.sleep(5)


if __name__ == '__main__':
    main()
