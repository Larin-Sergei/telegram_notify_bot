import asyncio
import logging
import os
from dotenv import load_dotenv
load_dotenv()
import re
import datetime
from collections import defaultdict
from typing import Callable, Dict, Any, Awaitable

import requests
from aiogram import Bot, Router, Dispatcher, types, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, BufferedInputFile, ReplyKeyboardMarkup, KeyboardButton, Message, TelegramObject, InputMediaPhoto, InputMediaDocument


from aiogram.types import ChatMemberUpdated

from db import Database

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
MAX_FILES = 10

def get_headers(token):
    headers = {
        'Private-Token': token
    }
    return headers

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-4852826917"))
GITLAB_HOST = os.getenv("GITLAB_HOST")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_PROJECT_ID = int(os.getenv("GITLAB_PROJECT_ID"))
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "table_name": os.getenv("DB_TABLE_NAME")
}
HEADERS = get_headers(GITLAB_TOKEN)
ISSUE_TYPE_NAMES = ["Задача", "Проблема"]

bot = Bot(token=TELEGRAM_TOKEN,default=DefaultBotProperties(parse_mode="HTML"))
keyboard_to_delete = types.ReplyKeyboardRemove()
router = Router()
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)
db = Database(
    dbname=DB_CONFIG['dbname'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port'])

def get_gitlab_users():
    """
    Получаем пользователя из GitLab по telegram_id
    :return: возвращает ответ по запросу
    """
    url = f'{GITLAB_HOST}/api/v4/users'
    response = requests.get(url, headers=HEADERS)
    logging.debug(f"Получение списка пользователей Gitlab: {response} {response.json()}")
    if response.status_code == 200:
        return response.json()
    return None

def get_user_from_users_list(users: list, user_id: int):
    """
    :param users: Список словарей с данными пользователя
    :param user_id: Идентификатор телеграма пользователя
    :return: None, если совпадений не найдено, иначе - словарь с данными пользователя
    """
    user_id = str(user_id)
    if users:
        for user in users:
            if user.get('skype') == user_id:
                return user
    else:
        return None

def create_personal_access_token(params: dict):
    """
    Создание токена доступа пользователю
    :param params: Словарь: user_id - id пользователя, name - название токена, scopes[] - права доступа,
    :return: Результат создания токена
    """
    url = f'{GITLAB_HOST}/api/v4/users/{params['user_id']}/personal_access_tokens'
    response = requests.post(url, headers=HEADERS, params=params)
    logging.debug(f"{response} {response.json()}")
    if response.status_code == 201:
        return response.json()
    return None

def get_gitlab_projects(headers):
    """
    Получить список проектов, доступных пользователю
    :param headers: Заголовок с токеном пользователя для запроса
    :return: список проектов
    """
    url = f'{GITLAB_HOST}/api/v4/projects'
    response = requests.get(url, headers=headers)
    logging.debug(f"{response} {response.json()}")
    if response.status_code == 200:
        return response.json()
    return None

def create_gitlab_issue(project_id: int, params: dict) -> dict | None:
    url = f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues"
    r = requests.post(url, headers=HEADERS, params=params)
    logging.debug(f"Create issue → {r.status_code} {r.text}")
    return r.json() if r.status_code == 201 else None

def make_row_keyboard(items: list[str], add_back_button: bool = True) -> ReplyKeyboardMarkup:
    buttons = [KeyboardButton(text=item) for item in items]
    if add_back_button:
        buttons.append(KeyboardButton(text="🔙 Назад"))
    keyboard = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_user(user_id: int, telegram_chat_id: int):
    user_db = db.get_user_by_telegram_id(user_id)
    if user_db:
        logging.info(f"Пользователь {user_id} в БД найден")
        return user_db
    else:
        logging.info(f"Пользователь {user_id} в БД не найден")
        gitlab_users = get_gitlab_users()
        if gitlab_users:
            gitlab_user = get_user_from_users_list(gitlab_users, user_id)
            if gitlab_user:
                params = {
                    'user_id': gitlab_user['id'],
                    'name': 'GitLab & Telegram Bot',
                    'scopes[]': 'api'
                }
                user_token_gitlab = create_personal_access_token(params)
                if user_token_gitlab:

                    logging.info(f"Пользователь {user_id} в Gitlab найден {gitlab_user}")

                    user_id_gitlab = gitlab_user['id']
                    user_username_gitlab = gitlab_user['username']

                    user_db = db.create_user(telegram_id=user_id, gitlab_id=user_id_gitlab,
                                             gitlab_login=user_username_gitlab,
                                             gitlab_token=user_token_gitlab['token'],
                                             telegram_chat_id=telegram_chat_id)
                    if user_db:
                        logging.info(f"Пользователь {user_id} создан в БД {user_db}")
                        return user_db
                    else:
                        logging.warning(f"Пользователь {user_id} не создан в БД")
                else:
                    logging.warning(
                        f"Для пользователя {user_id} не создан токен Gitlab и создание пользователя в БД прекращено")
            else:
                logging.info(f"Пользователь {user_id} в Gitlab не найден {gitlab_user}")
        else:
            logging.warning(f"Не удалось получить список пользователей Gitlab")
        return None


class AlbumMiddleware(BaseMiddleware):
    def __init__(self, latency: float = 0.3):
        self.latency = latency
        self.albums = defaultdict(list)

    async def __call__(
            self,
            handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
            event: Message,
            data: Dict[str, Any]
    ) -> Any:
        # Если сообщение не часть альбома
        if not event.media_group_id:
            return await handler(event, data)

        # Добавляем сообщение в альбом
        self.albums[event.media_group_id].append(event)

        # Задержка для сбора всего альбома
        await asyncio.sleep(self.latency)

        # Если это последнее сообщение в альбоме
        if event.media_group_id in self.albums:
            data["album"] = self.albums.pop(event.media_group_id)
            return await handler(event, data)

album_middleware = AlbumMiddleware()
dp.message.middleware(album_middleware)

class CreateIssue(StatesGroup):
    select_title = State()  # Указываем заголовок задачи
    select_description = State()  # Указываем описание
    add_files = State()  # Добавляем файлы
    send_issue = State()  # Отправляем ли задачу/проблему

class CreateGitlabUser(StatesGroup):
    create_gitlab_user = State()
    set_gitlab_login = State()

class ReopenIssue(StatesGroup):
    enter_comment = State()
    add_files = State()

class AttachFiles(StatesGroup):
    sending = State()

class CommentIssue(StatesGroup):
    enter_comment = State()
    add_files = State()

@router.callback_query(lambda c: c.data.startswith("attach:"))
async def attach_files_callback(callback: types.CallbackQuery, state: FSMContext):
    _, project_id, issue_iid = callback.data.split(":")
    await state.update_data(
        project_id=int(project_id),
        issue_iid=int(issue_iid),
        attach_files=[]
    )
    await state.set_state(AttachFiles.sending)
    await callback.message.answer(
        "Прикрепите файлы к обращению или нажмите «Готово», когда закончите",
        reply_markup=make_row_keyboard(['Готово'], add_back_button=False)
    )
    await callback.answer()

@router.message(StateFilter(AttachFiles.sending), F.media_group_id)
async def collect_attach_media_group(
    message: types.Message,
    album: list[types.Message],
    state: FSMContext
):
    data = await state.get_data()
    files = data.get('attach_files', [])

    for msg in album:
        file_obj = msg.document or msg.photo[-1]
        file_info = await bot.get_file(file_obj.file_id)
        downloaded = await bot.download_file(file_info.file_path)
        content = downloaded.read()

        if len(content) > MAX_FILE_SIZE:
            await msg.answer(f"🚫 Файл {getattr(file_obj, 'file_name', file_obj.file_id)} слишком большой")
            continue
        if len(files) >= MAX_FILES:
            await msg.answer("🚫 Достигнут лимит файлов")
            break

        files.append({
            'file_name': getattr(file_obj, 'file_name', f"file_{file_obj.file_id}"),
            'file_data': content,
            'mime_type': file_obj.mime_type
        })

    await state.update_data(attach_files=files)
    await message.answer(f"📥 Принято {len(album)} файлов из альбома")

@router.message(StateFilter(AttachFiles.sending), F.document | F.photo)
async def collect_attach_files(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data['attach_files']
    file_obj = message.document or message.photo[-1]
    file_info = await bot.get_file(file_obj.file_id)
    downloaded = await bot.download_file(file_info.file_path)
    content = downloaded.read()

    if len(content) > MAX_FILE_SIZE:
        return await message.answer(f"🚫 Файл слишком большой (макс {MAX_FILE_SIZE//1024//1024} MB)")
    if len(files) >= MAX_FILES:
        return await message.answer(f"🚫 Максимум {MAX_FILES} файлов")

    files.append({
        'file_name': getattr(file_obj, 'file_name', f"file_{file_obj.file_id}"),
        'file_data': content,
        'mime_type': file_obj.mime_type
    })
    await state.update_data(attach_files=files)
    await message.answer(f"📥 Принято: {files[-1]['file_name']}")

@router.message(StateFilter(AttachFiles.sending), F.text == 'Готово')
async def finish_attach_files(message: types.Message, state: FSMContext):
    data = await state.get_data()
    project_id = data['project_id']
    issue_iid = data['issue_iid']
    files = data.get('attach_files', [])

    headers = HEADERS

    markdowns = []
    for f in files:
        resp = requests.post(
            f"{GITLAB_HOST}/api/v4/projects/{project_id}/uploads",
            headers=headers,
            files={'file': (f['file_name'], f['file_data'], f['mime_type'])}
        )
        if resp.status_code == 201:
            markdowns.append(resp.json()['markdown'])
        else:
            await message.answer(f"⚠️ Ошибка загрузки {f['file_name']}")

    if markdowns:
        body = "<b>Прикрепленные файлы:</b>\n" + "\n".join(markdowns)
        notes_url = f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}/notes"
        note_resp = requests.post(notes_url, headers=headers, json={'body': body})
        if note_resp.status_code == 201:
            await message.answer("✅ Файлы успешно прикреплены.", parse_mode='HTML')
        else:
            await message.answer("❌ Не удалось добавить файлы к обращению.")
    else:
        await message.answer("ℹ️ Нет файлов для прикрепления.")

    await state.clear()
    await cmd_start(message, state)

@router.message(StateFilter(ReopenIssue.enter_comment))
async def process_reopen_comment(message: types.Message, state: FSMContext):
    await state.update_data(
        comment_text=message.text,
        attach_files=[]
    )
    await message.answer(
        "✏️ Ваш комментарий сохранён.\n\n"
        "Прикрепите файлы или фотографии к комментарию, или нажмите «Готово».",
        reply_markup=make_row_keyboard(['Готово'], add_back_button=False)
    )
    await state.set_state(ReopenIssue.add_files)

@router.callback_query(F.data.startswith("reopen:"))
async def reopen_issue_callback(callback: types.CallbackQuery, state: FSMContext):
    _, project_id, issue_iid = callback.data.split(":")
    project_id = int(project_id)
    issue_iid = int(issue_iid)
    await state.update_data(project_id=project_id, issue_iid=issue_iid)

    db.add_subscription(callback.from_user.id, project_id, issue_iid)

    await state.set_state(ReopenIssue.enter_comment)
    await callback.message.answer("✍️ Введите комментарий, чтобы вернуть обращение на доработку:")
    await callback.answer()

@router.message(StateFilter(ReopenIssue.add_files), F.document | F.photo)
async def collect_reopen_files(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data['attach_files']
    file_obj = message.document or message.photo[-1]
    file_info = await bot.get_file(file_obj.file_id)
    downloaded = await bot.download_file(file_info.file_path)
    content = downloaded.read()

    if len(content) > MAX_FILE_SIZE:
        return await message.answer(f"🚫 Файл слишком большой (макс {MAX_FILE_SIZE//1024//1024} MB)")
    if len(files) >= MAX_FILES:
        return await message.answer(f"🚫 Максимум {MAX_FILES} файлов")

    files.append({
        'file_name': getattr(file_obj, 'file_name', f"file_{file_obj.file_id}"),
        'file_data': content,
        'mime_type': file_obj.mime_type
    })
    await state.update_data(attach_files=files)
    await message.answer(f"📥 Принято: {files[-1]['file_name']}")

@router.message(StateFilter(ReopenIssue.add_files), F.text == 'Готово')
async def finish_reopen(message: types.Message, state: FSMContext):
    data = await state.get_data()
    project_id = data["project_id"]
    issue_iid = data["issue_iid"]
    comment = data["comment_text"]
    files = data.get("attach_files", [])

    headers = HEADERS

    markdowns = []
    for f in files:
        resp = requests.post(
            f"{GITLAB_HOST}/api/v4/projects/{project_id}/uploads",
            headers=headers,
            files={'file': (f['file_name'], f['file_data'], f['mime_type'])}
        )
        if resp.status_code == 201:
            markdowns.append(resp.json()['markdown'])

    body = comment
    if markdowns:
        body += "\n\n<b>Прикреплённые файлы:</b>\n" + "\n".join(markdowns)

    notes_url = f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}/notes"
    note_resp = requests.post(notes_url, headers=headers, json={"body": body})
    if note_resp.status_code != 201:
        await message.reply("❌ Не удалось отправить комментарий с вложениями.")
        await state.clear()
        return

    issue_url = f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}"

    payload = {"state_event": "reopen",
               "labels": "На доработке"}

    reopen_resp = requests.put(issue_url, headers=headers, json=payload)

    if reopen_resp.status_code != 200:
        await message.reply("❌ Не удалось вернуть обращение на доработку.")
    else:
        db.mark_issue_unnotified(project_id, issue_iid)
    await state.clear()

@router.message(StateFilter(CommentIssue.add_files), F.document | F.photo)
async def collect_comment_files(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data['attach_files']
    file_obj = message.document or message.photo[-1]
    file_info = await bot.get_file(file_obj.file_id)
    content = (await bot.download_file(file_info.file_path)).read()
    if len(content) > MAX_FILE_SIZE:
        return await message.answer("🚫 Файл слишком большой")
    if len(files) >= MAX_FILES:
        return await message.answer("🚫 Достигнут лимит файлов")
    files.append({
        'file_name': getattr(file_obj, 'file_name', f"file_{file_obj.file_id}"),
        'file_data': content,
        'mime_type': file_obj.mime_type
    })
    await state.update_data(attach_files=files)
    await message.answer(f"📥 Принято: {files[-1]['file_name']}")

@router.message(StateFilter(CommentIssue.add_files), F.text == 'Готово')
async def finish_comment(message: types.Message, state: FSMContext):
    data = await state.get_data()
    project_id = data["project_id"]
    issue_iid = data["issue_iid"]
    comment = data["comment_text"]
    files = data.get("attach_files", [])
    headers = HEADERS

    markdowns = []
    for f in files:
        resp = requests.post(
            f"{GITLAB_HOST}/api/v4/projects/{project_id}/uploads",
            headers=headers,
            files={'file': (f['file_name'], f['file_data'], f['mime_type'])}
        )
        if resp.status_code == 201:
            markdowns.append(resp.json()['markdown'])

    body = comment
    if markdowns:
        body += "\n\n<b>Прикреплённые файлы:</b>\n" + "\n".join(markdowns)

    notes_url = f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}/notes"
    note_resp = requests.post(notes_url, headers=headers, json={"body": body})
    if note_resp.status_code != 201:
        await message.reply("❌ Не удалось отправить комментарий с вложениями.")
    else:
        await message.reply("✅ Комментарий добавлен к обращению.")
    await state.clear()

def notify_issue_updated(project_id: int, issue_iid: int, message_text: str):
    subscribers = db.get_subscribers(project_id, issue_iid)
    for telegram_id in subscribers:
        try:
            asyncio.create_task(bot.send_message(chat_id=telegram_id, text=message_text))
        except Exception as e:
            logging.warning(f"Failed to notify user {telegram_id}: {e}")

async def show_issue_add_files(message: types.Message, state: FSMContext):
    await message.reply(text=f'Прикрепите вложения', reply_markup=make_row_keyboard(['Продолжить']))

async def show_issue_select_description(message: types.Message, state: FSMContext):
    await message.reply(text=f'Введите описание', reply_markup=make_row_keyboard([]))


@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    rows = db.get_all_tracked_issues()
    for project_id, issue_iid, chat_id, last_known, _ in rows:
        if chat_id != message.chat.id:
            continue

        issue_url = f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}"
        resp = requests.get(issue_url, headers=HEADERS)
        if resp.status_code != 200:
            continue
        issue = resp.json()

        if issue["state"] == "closed":
            closed_at = issue.get("closed_at")
            closed_dt = None
            if closed_at:
                closed_dt = datetime.datetime.fromisoformat(closed_at.rstrip("Z"))

            notes = requests.get(
                f"{issue_url}/notes",
                params={"order_by": "created_at", "sort": "desc"},
                headers=HEADERS
            ).json()

            assignees = issue.get("assignees") or []
            if assignees:
                assignee = assignees[0]
                assignee_id = assignee["id"]
                assignee_name = assignee.get("name", "—")
            else:
                single = issue.get("assignee") or {}
                assignee_id = single.get("id")
                assignee_name = single.get("name", "—")

            closing_comment = None
            closing_comment_id = None
            if closed_dt:
                for n in notes:
                    if n.get("system"):
                        continue
                    note_dt = datetime.datetime.fromisoformat(n["created_at"].rstrip("Z"))
                    if abs((note_dt - closed_dt).total_seconds()) < 1:
                        closing_comment = n["body"].strip()
                        closing_comment_id = n["id"]
                        break

            if closing_comment is None:
                non_system = [n for n in notes if not n.get("system", False)]
                if non_system and assignee_id and non_system[0]["author"]["id"] == assignee_id:
                    closing_comment = non_system[0]["body"].strip()
                    closing_comment_id = non_system[0]["id"]

            lines = [
                f"Обращение #{issue_iid} ({issue['state']}) <b>{issue['title']}</b> передано на приемку",
                f"Исполнитель: {assignee_name}",
                "",
                "Пожалуйста, проверьте результаты по обращению — "
                "если остались вопросы, верните на доработку, "
                "если вопросов нет, нажмите кнопку «Принять».",
            ]
            if closing_comment:
                lines += [
                    "",
                    "<b>Комментарий</b> исполнителя:",
                    closing_comment
                ]
            detail_text = "\n".join(lines)

            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Принять",    callback_data=f"ack:{project_id}:{issue_iid}"),
                InlineKeyboardButton(text="Вернуть на доработку", callback_data=f"reopen:{project_id}:{issue_iid}")
            ]])

            await state.clear()
            await message.answer(detail_text, reply_markup=kb, parse_mode="HTML")
            return

        raw_desc = issue.get('description') or ""
        sanitized = re.sub(r'<details>.*?</details>', "", raw_desc, flags=re.DOTALL | re.IGNORECASE)
        body_only = strip_metadata(sanitized) or "—"

        all_notes = requests.get(f"{issue_url}/notes", headers=HEADERS).json()
        user_notes = [n for n in all_notes if not n.get("system", False)]
        user_notes.sort(key=lambda n: n["created_at"])
        last_three = user_notes[-3:]
        if last_three:
            comments = []
            for note in last_three:
                dt = datetime.datetime.fromisoformat(note["created_at"].rstrip("Z"))
                dt_str = dt.strftime("%d.%m.%Y %H:%M")
                comments.append(f"<i>{note['author']['name']}</i>, {dt_str}\n{note['body']}")
            comments_text = "\n\n".join(comments)
        else:
            comments_text = "Комментариев нет."

        in_progress = (f"<b>Текущее обращение #{issue_iid} ({issue['state']}) – {issue['title']}</b>\n\n"
                f"{body_only}\n\n"
                f"{comments_text}")
        kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Оставить комментарий", callback_data=f"comment:{project_id}:{issue_iid}")]])
        await state.clear()
        await message.answer(in_progress, reply_markup=kb, parse_mode="HTML")
        return

    await prompt_issue_creation(message, state)

@router.callback_query(lambda c: c.data.startswith("issue:"))
async def issue_selected_callback(callback: types.CallbackQuery):
    _, project_id, issue_iid = callback.data.split(":")
    project_id, issue_iid = int(project_id), int(issue_iid)

    user_db = get_user(callback.from_user.id, callback.message.chat.id)
    if not user_db:
        await callback.message.answer("Пользователь не найден в базе данных.")
        await callback.answer()
        return

    headers = HEADERS
    issue_url = f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}"
    resp = requests.get(issue_url, headers=headers)
    if resp.status_code != 200:
        await callback.message.answer("Не удалось получить данные обращения.")
        await callback.answer()
        return
    issue = resp.json()

    raw_desc = issue.get('description') or ""
    sanitized_desc = re.sub(r'<details>.*?</details>', '', raw_desc, flags=re.DOTALL|re.IGNORECASE)
    body_only = strip_metadata(sanitized_desc) or "—"

    attachments = re.findall(r'\[([^\]]+)\]\((/uploads/[^\)]+)\)', raw_desc)

    notes_url = f"{issue_url}/notes"
    notes_resp = requests.get(notes_url, headers=headers)
    latest = "Комментариев нет."
    if notes_resp.status_code == 200:
        notes = sorted(notes_resp.json(), key=lambda n: n['created_at'], reverse=True)
        if notes:
            latest = notes[0].get('body', latest)

    issue_text = (
        f"<b>Обращение #{issue['iid']}</b>\n"
        f"Название: {issue['title']}\n"
        f"Описание: {body_only}\n"
        f"Статус: {issue['state']}\n"
        f"Автор: {issue['author']['name']}\n"
        f"<b>Последний комментарий:</b>\n{latest}"
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
                [InlineKeyboardButton(
                    text="Оставить комментарий",
                    callback_data=f"comment:{project_id}:{issue_iid}"),
                InlineKeyboardButton(
                    text="Прикрепить файлы",
                    callback_data=f"attach:{project_id}:{issue_iid}")
            ]])
    await callback.message.answer(issue_text, reply_markup=keyboard, parse_mode='HTML')

    for label, path in attachments:
        file_url = f"{GITLAB_HOST}{path}"
        resp = requests.get(file_url, headers=HEADERS)
        if resp.status_code == 200:
            tg_file = BufferedInputFile(
                resp.content,
                filename=label)
            await callback.message.answer_document(
                document=tg_file,
                caption=label)
        else:
            await callback.message.answer(f"⚠️ Не удалось загрузить вложение {label}")
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("comment:"))
async def comment_issue_callback(callback: types.CallbackQuery, state: FSMContext):
    _, project_id, issue_iid = callback.data.split(":")
    project_id = int(project_id)
    issue_iid = int(issue_iid)
    await state.update_data(project_id=project_id, issue_iid=issue_iid)

    db.add_subscription(callback.from_user.id, project_id, issue_iid)

    await state.set_state(CommentIssue.enter_comment)
    await callback.message.answer("✍️ Введите комментарий к обращению:")
    await callback.answer()

@router.message(StateFilter(CommentIssue.enter_comment))
async def process_comment(message: types.Message, state: FSMContext):
    data = await state.get_data()
    await state.update_data(comment_text=message.text, attach_files=[])
    await message.answer(
        "✏️ Ваш комментарий сохранён.\n\n"
        "Прикрепите файлы или фотографии к комментарию, или нажмите «Готово».",
        reply_markup=make_row_keyboard(['Готово'], add_back_button=False)
    )
    await state.set_state(CommentIssue.add_files)

@router.message(StateFilter(CreateIssue.select_title))
async def cmd_select_title(message: types.Message, state: FSMContext):
    if message.text == '🔙 Назад':
        return await message.reply(
            "📝 Укажите тему обращения:",
            reply_markup=make_row_keyboard([], add_back_button=False))

    await add_state_to_history(state, await state.get_state())
    await state.update_data(issue_title=message.text)
    await message.reply(
        text = 'Введите описание',
        reply_markup = make_row_keyboard([], add_back_button=True))
    await state.set_state(CreateIssue.select_description)

@router.message(StateFilter(CreateIssue.select_description))
async def cmd_select_description(message: types.Message, state: FSMContext):
    await add_state_to_history(state, await state.get_state())
    await state.update_data(issue_description=message.text)
    await state.update_data(files=[])
    if message.text == '🔙 Назад':
        await message.reply(
            "📝 Укажите тему обращения:",
            reply_markup=make_row_keyboard([], add_back_button=False))
        return await state.set_state(CreateIssue.select_title)
    await message.reply(text=f'Прикрепите вложения', reply_markup=make_row_keyboard(['Продолжить'], add_back_button=True))
    await state.set_state(CreateIssue.add_files)

@router.message(StateFilter(CreateIssue.add_files), F.media_group_id)
async def handle_media_group(message: types.Message, album: list[types.Message], state: FSMContext):
    """Обработка альбома из нескольких файлов"""
    await add_state_to_history(state, await state.get_state())
    data = await state.get_data()
    files = data.get('files', [])
    for msg in album:
        file = None
        file_info = None
        if msg.document:
            file = msg.document
            file_info = await bot.get_file(file.file_id)
            file_type = "document"
        elif msg.photo:
            file = msg.photo[-1]
            file_info = await bot.get_file(file.file_id)
            file_type = "photo"

        if file_info:
            downloaded_file = await bot.download_file(file_info.file_path)
            file_data = downloaded_file.read()

            file_size = len(file_data)
            if file_size > MAX_FILE_SIZE:
                await msg.answer(
                    f"Файл {file.file_name if file else 'Не найдено'} слишком большой (макс. {MAX_FILE_SIZE // 1024 // 1024} MB)")

            if len(files) >= MAX_FILES:
                await msg.answer(f"Максимум {MAX_FILES} файлов")

            files.append({
                'file_name': file.file_name if hasattr(file, 'file_name') else f"photo_{file.file_id}.jpg",
                'file_data': file_data,
                'mime_type': file.mime_type if hasattr(file, 'mime_type') else "image/jpeg"
            })
            await state.update_data(files=files)
@router.message(StateFilter(CreateIssue.add_files), F.document | F.photo)
async def collect_files(message: types.Message, state: FSMContext):
    await add_state_to_history(state, await state.get_state())
    data = await state.get_data()
    files = data.get('files', [])
    file = None
    file_info = None
    if message.document:
        file = message.document
        file_info = await bot.get_file(file.file_id)
    elif message.photo:
        file = message.photo[-1]
        file_info = await bot.get_file(file.file_id)

    if file_info:
        downloaded_file = await bot.download_file(file_info.file_path)
        file_data = downloaded_file.read()
        file_size = len(file_data)
        if file_size > MAX_FILE_SIZE:
            await message.answer(f"Файл {file.file_name if file else 'Не найдено'} слишком большой (макс. {MAX_FILE_SIZE // 1024 // 1024} MB)")
        if len(files) >= MAX_FILES:
            await message.answer(f"Максимум {MAX_FILES} файлов")
        files.append({
            'file_name': file.file_name if hasattr(file, 'file_name') else f"photo_{file.file_id}.jpg",
            'file_data': file_data,
            'mime_type': file.mime_type if hasattr(file, 'mime_type') else "image/jpeg"
        })
        await state.update_data(files=files)

    if message.text == 'Продолжить':
        send_or_cancel = ["Отправить", "Отменить"]
        await message.answer(text=f'~~~~ Введенные данные: ~~~~\n'
                                 f'<b>Заголовок:</b> {data.get('issue_title', 'Не указано')}\n'
                                 f'<b>Описание:</b> {data.get('issue_description', 'Не указано')}', parse_mode='HTML',
                            reply_markup=make_row_keyboard(send_or_cancel))
        await state.set_state(CreateIssue.send_issue)

@router.message(StateFilter(CreateIssue.send_issue))
async def cmd_send_issue(message: types.Message, state: FSMContext):
    if message.text != 'Отправить':
        await message.reply("🚫 Операция отменена.")
        return await cmd_start(message, state)

    data = await state.get_data()
    usr  = message.from_user

    header_lines: list[str] = []
    if usr.username:
        header_lines.append(f"Никнейм: @{usr.username}")
    header_lines.append(f"ID: {usr.id}")
    if usr.full_name:
        header_lines.append(f"Имя: {usr.full_name}")
    phone = data.get("phone_number")
    if phone:
        header_lines.append(f"Телефон: {phone}")

    header = "\n\n".join(header_lines)
    body = data["issue_description"]
    full_descr = f"{header}\n\n{body}"
    params = {
        "title": data["issue_title"],
        "description": full_descr,
        "issue_type": "incident",
    }
    gitlab_issue = create_gitlab_issue(GITLAB_PROJECT_ID, params)

    if gitlab_issue:
        await message.reply(f"✅ Обращение зарегистрировано.")
        db.create_tracked_issue(
            project_id=GITLAB_PROJECT_ID,
            issue_iid=gitlab_issue["iid"],
            telegram_chat_id=message.chat.id
        )
        db.add_subscription(message.from_user.id, GITLAB_PROJECT_ID, gitlab_issue["iid"])
        try:
            await bot.send_message(
                GROUP_CHAT_ID,
                f"🚀 Новый запрос #{gitlab_issue['iid']}: <b>{data['issue_title']}</b>\n"
                f"{GITLAB_HOST}/{GITLAB_PROJECT_ID}/-/issues/{gitlab_issue['iid']}",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.warning(f"Failed to notify group {GROUP_CHAT_ID}: {e}")
    else:
        await message.reply("❌ Ошибка при создании обращения.")

    await state.clear()

@router.callback_query(lambda c: c.data.startswith("ack:"))
async def acknowledge_issue_callback(callback: types.CallbackQuery):
    _, project_id, issue_iid = callback.data.split(":")
    project_id, issue_iid = int(project_id), int(issue_iid)

    issue_url = f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}"
    requests.put(issue_url, headers=HEADERS, json={"labels": ""})
    db.delete_tracked_issue(project_id, issue_iid)

    await callback.message.answer("✅ Обращение закрыто. Спасибо!", parse_mode="HTML")
    await callback.answer()

@router.message(StateFilter(CreateIssue.add_files), F.text == 'Продолжить')
async def cmd_ready_to_send(message: types.Message, state: FSMContext):
    data = await state.get_data()
    files = data.get('files', [])
    summary = (
            f"<b>Заголовок:</b> {data['issue_title']}\n"
             f"<b>Описание:</b> {data['issue_description']}\n"
             f"<b>Файлов:</b> {len(files)}")
    await message.reply(
        summary + "\n\nОтправить задачу?",
        reply_markup=make_row_keyboard(["Отправить", "Отменить"]))
    await state.set_state(CreateIssue.send_issue)

async def monitor_closed_issues():
    logging.info("🚨 monitor_closed_issues has started")
    while True:
        for project_id, issue_iid, chat_id in db.get_unnotified_issues():
            issue_url = f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}"
            r = requests.get(issue_url, headers=HEADERS)
            if r.status_code != 200:
                continue
            issue = r.json()
            if issue.get("state") != "closed":
                continue

            requests.put(issue_url, headers=HEADERS, json={"labels": "На проверке"})

            closed_at = issue.get("closed_at")
            closed_dt = None
            if closed_at:
                closed_dt = datetime.datetime.fromisoformat(closed_at.rstrip("Z"))

            notes = requests.get(
                f"{issue_url}/notes",
                params={"order_by": "created_at", "sort": "desc"},
                headers=HEADERS
            ).json()

            assignees = issue.get("assignees") or []
            if assignees:
                assignee = assignees[0]
                assignee_id = assignee.get("id")
                assignee_name = assignee.get("name", "—")
            else:
                single = issue.get("assignee") or {}
                assignee_id = single.get("id")
                assignee_name = single.get("name", "—")

            closing_comment = None
            closing_comment_id = None
            if closed_dt:
                for n in notes:
                    if n.get("system"):
                        continue
                    note_dt = datetime.datetime.fromisoformat(n["created_at"].rstrip("Z"))
                    if abs((note_dt - closed_dt).total_seconds()) < 1:
                        closing_comment = n["body"].strip()
                        closing_comment_id = n["id"]
                        break

            if closing_comment is None:
                non_system = [n for n in notes if not n.get("system", False)]
                if non_system and assignee_id and non_system[0]["author"]["id"] == assignee_id:
                    closing_comment = non_system[0]["body"].strip()
                    closing_comment_id = non_system[0]["id"]

            lines = [
                f"Обращение #{issue_iid} ({issue['state']}) <b>{issue['title']}</b> передано на приемку",
                f"Исполнитель: {assignee_name}",
                "",
                "Пожалуйста, проверьте результаты по обращению — "
                "если остались вопросы, верните на доработку, "
                "если вопросов нет, нажмите кнопку «Принять».",]
            if closing_comment:
                lines += ["",
                          "<b>Комментарий исполнителя:</b>",
                          closing_comment]
            detail_text = "\n".join(lines)

            if closing_comment_id is not None:
                db.update_last_note_id(project_id, issue_iid, closing_comment_id)

            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Принять",    callback_data=f"ack:{project_id}:{issue_iid}"),
                InlineKeyboardButton(text="Вернуть на доработку", callback_data=f"reopen:{project_id}:{issue_iid}")
            ]])
            await bot.send_message(chat_id, detail_text, parse_mode="HTML", reply_markup=kb)
            db.mark_issue_notified(project_id, issue_iid)

        await asyncio.sleep(60)

async def monitor_auto_ack():
    while True:
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
        rows = db.get_notified_unacked_older_than(cutoff)
        for project_id, issue_iid, chat_id in rows:
            db.delete_tracked_issue(project_id, issue_iid)
            await bot.send_message(
                chat_id,
                "⏰ Вы не ответили в течение 24 часов — задача закрывается автоматически.",
                parse_mode="HTML")
        await asyncio.sleep(3600)

async def prompt_issue_creation(message: Message, state: FSMContext):
    await state.clear()
    await message.reply(
        "📝 Укажите тему обращения:",
        reply_markup=make_row_keyboard([], add_back_button=False))
    await state.set_state(CreateIssue.select_title)

@router.message(StateFilter(CreateIssue.select_description), F.text == '🔙 Назад')
async def go_back_to_title(message: Message, state: FSMContext):
    await state.update_data(issue_description=None)
    await message.reply(
        "📝 Укажите тему обращения:",
        reply_markup=make_row_keyboard([], add_back_button=False))
    await state.set_state(CreateIssue.select_title)

@router.message(StateFilter(CreateIssue.add_files), F.text == '🔙 Назад')
async def go_back_to_description(message: Message, state: FSMContext):
    await state.update_data(files=[])
    await message.reply(
        "Введите описание",
        reply_markup=make_row_keyboard([], add_back_button=True))
    await state.set_state(CreateIssue.select_description)

async def monitor_new_comments():
    logging.info("🚨 monitor_new_comments has started")
    link_regex = re.compile(r'\[([^\]]+)\]\((/uploads/[^)]+)\)')
    img_regex  = re.compile(r'!\[[^\]]*\]\((/uploads/[^)]+)\)')
    while True:
        for project_id, issue_iid, chat_id, last_known, _ in db.get_all_tracked_issues():
            r_issue = requests.get(
                f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}",
                headers=HEADERS)

            if r_issue.status_code != 200:
                continue
            issue = r_issue.json()

            if issue.get("state") == "closed":
                continue

            r_notes = requests.get(
                f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}/notes",
                params={"order_by": "created_at", "sort": "asc"},
                headers=HEADERS)

            if r_notes.status_code != 200:
                continue
            notes = [n for n in r_notes.json() if not n.get("system", False)]

            new_notes = [n for n in notes if n["id"] > last_known]
            if not new_notes:
                continue

            new_last = max(n["id"] for n in new_notes)
            db.update_last_note_id(project_id, issue_iid, new_last)

            owner_id = issue["author"]["id"]
            recips = set(db.get_subscribers(project_id, issue_iid))
            owner = db.get_user_by_gitlab_id(owner_id)
            if owner:
                recips.add(owner["telegram_chat_id"])

            for note in new_notes:
                if note["author"]["id"] == owner_id:
                    continue

                body = link_regex.sub("", note["body"])
                body = img_regex.sub("", body).strip()

                caption = (
                    f"🔔 <b>Новый комментарий</b> по обращению #{issue_iid}\n\n"
                    f"{body}\n\n"
                    f"<i>Автор: {note['author']['name']}</i>"
                )

                attachments = link_regex.findall(note["body"])
                attachments += [
                    (alt or os.path.basename(p), p)
                    for p in img_regex.findall(note["body"])
                    for alt, p in [("", p)]
                ]

                if attachments:
                    media = []
                    for idx, (label, path) in enumerate(attachments):
                        resp = requests.get(f"{GITLAB_HOST}{path}", headers=HEADERS, stream=True)
                        if resp.status_code != 200:
                            continue
                        fn = label or os.path.basename(path).lstrip("_")
                        buf = BufferedInputFile(resp.content, filename=fn)
                        if resp.headers.get("Content-Type", "").startswith("image/"):
                            item = InputMediaPhoto(media=buf,
                                                   caption=caption if idx == 0 else fn,
                                                   parse_mode="HTML")
                        else:
                            item = InputMediaDocument(media=buf,
                                                      caption=caption if idx == 0 else fn,
                                                      parse_mode="HTML")
                        media.append(item)
                    for cid in recips:
                        await bot.send_media_group(cid, media)
                else:
                    for cid in recips:
                        await bot.send_message(cid, caption, parse_mode="HTML")

        await asyncio.sleep(60)

def strip_metadata(description: str) -> str:
    prefixes = ("Никнейм:", "ID:", "Имя:", "Телефон:")
    lines = description.splitlines()
    i = 0
    while i < len(lines):
        text = lines[i].strip()
        if not text or any(text.startswith(p) for p in prefixes):
            i += 1
            continue
        break
    return "\n".join(lines[i:]).strip()

async def monitor_assignment_changes():
    logging.info("🚨 monitor_assignment_changes has started")
    while True:
        for project_id, issue_iid, chat_id, _last_note, last_assignee in db.get_all_tracked_issues():
            resp = requests.get(
                f"{GITLAB_HOST}/api/v4/projects/{project_id}/issues/{issue_iid}",
                headers=HEADERS)
            if resp.status_code != 200:
                continue

            issue = resp.json()
            assignees = issue.get("assignees") or []
            curr_id = assignees[0]["id"] if assignees else None

            if curr_id is not None and curr_id != last_assignee:
                if last_assignee is None:
                    text = "🔔 По обращению назначен исполнитель"
                else:
                    text = "🔔 Назначен новый исполнитель"

                try:
                    await bot.send_message(chat_id, text, parse_mode="HTML")
                except Exception as e:
                    logging.warning(f"Failed to notify assignment change: {e}")

                db.update_last_assignee_id(project_id, issue_iid, curr_id)

        await asyncio.sleep(60)

@router.message(StateFilter(CreateGitlabUser.create_gitlab_user))
async def cmd_create_gitlab_user(message: types.Message, state: FSMContext):
    await add_state_to_history(state, await state.get_state())
    if message.text == 'Да':
        await message.reply(f'Функционал в разработке', reply_markup=make_row_keyboard([]))
    elif message.text == 'Нет':
        await message.reply(f'Функционал в разработке', reply_markup=make_row_keyboard([]))
    else:
        await message.reply(text=f'Команда не распознана', reply_markup=make_row_keyboard([]))

@router.message(StateFilter(CreateGitlabUser.set_gitlab_login))
async def cmd_set_gitlab_login(message: types.Message, state: FSMContext):
    await add_state_to_history(state, await state.get_state())
    await state.clear()

async def add_state_to_history(state: FSMContext, new_state: str):
    data = await state.get_data()
    history = data.get("state_history", [])
    if not history or history[-1] != new_state:
        history.append(new_state)
        await state.update_data(state_history=history)


@router.my_chat_member()
async def on_added_to_group(my_chat_member: ChatMemberUpdated):
    if my_chat_member.new_chat_member.status in ("member", "administrator"):
        chat = my_chat_member.chat
        # record chat.id in your groups table
        db.register_group(chat.id)
        await bot.send_message(
            chat.id,
            "👋 Thanks for adding me! I’m now watching this group for GitLab issues."
        )

@router.message()
async def tracer(message: types.Message):
    logging.info(f"⏳ got {message.text!r} in chat {message.chat.id} ({message.chat.type})")

async def main():
    asyncio.create_task(monitor_closed_issues())
    asyncio.create_task(monitor_new_comments())
    await dp.start_polling(bot)

@dp.startup()
async def on_startup():
    logging.info("🔌 on_startup: scheduling background tasks")
    asyncio.create_task(monitor_closed_issues())
    asyncio.create_task(monitor_new_comments())
    asyncio.create_task(monitor_assignment_changes())
    asyncio.create_task(monitor_auto_ack())

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    dp.run_polling(bot)
