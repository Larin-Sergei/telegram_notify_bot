import logging
import os
import re

import requests
from fastapi import FastAPI, Body

from db import Database

app = FastAPI()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "table_name": os.getenv("DB_TABLE_NAME")
}

ISSUE_ACTION_TRANSLATE = {
    'close': 'закрыта',
    'reopen': 'открыта'
}

db = Database(
    dbname=DB_CONFIG['dbname'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port'])

def send_telegram_message(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    requests.post(url, json=payload)


def parse_comment(comment):
    """
    Разбирает строку и возвращает словарь с пользователями и сообщением

    :param comment: Строка в формате "@user1, @user2 сообщение"
    :return: Словарь с ключами "users" и "message"
    """
    pattern = r'@[\w\.]+'
    users = re.findall(pattern, comment)

    message = re.sub(pattern, '', comment).strip()

    return {"users": users, "message": message}

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/webhook")
async def say_hello(data = Body()):
    logging.info(data)
    print(data)
    event_type = data.get('event_type')

    if event_type == 'note':
        data = parse_comment(data)
    elif event_type == 'issue':
        if data['object_attributes']['action'] in ['close', 'reopen']:
            comment = data['object_attributes']['note']
            comment_data = was_changed_issue_state(comment)

    return {"message": data}

def was_changed_issue_state(data):
    issue_type = ''
    if data['object_attributes']['type'] == 'Incident':
        issue_type = 'Проблема'
    elif data['object_attributes']['type'] == 'Issue':
        issue_type = 'Задача'

    issue_action = ''
    if data['object_attributes']['action'] == 'close':
        issue_action = 'закрыта'
    elif data['object_attributes']['action'] == 'reopen':
        issue_action = 'открыта'

    issue_title = data['object_attributes']['title']

    if issue_type and issue_action:
        message = f"<b>{issue_type}</b>#{data['object_attributes']['iid']} {issue_title} <b>{issue_action}</b>"
        gitlab_user_id = data['user']['id']
        user_db = db.get_user_by_gitlab_id(gitlab_user_id)
        send_telegram_message(user_db['telegram_chat_id'], message)

@app.get("/ping")
async def ping():
    return {"message": 'test'}
