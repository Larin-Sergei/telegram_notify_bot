import logging
import datetime

import psycopg2
from psycopg2 import sql

import os
from dotenv import load_dotenv

load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "table_name": os.getenv("DB_TABLE_NAME")
}

class Database:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def check_table_exists(self, table_name, schema='public'):
        """Проверяет существование таблицы в указанной схеме."""
        query = sql.SQL("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (schema, table_name))  # Учитываем регистр
            return cur.fetchone()[0]

    def create_users_table(self):
        """Создаёт таблицу Users с необходимыми столбцами."""
        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                telegram_id BIGINT PRIMARY KEY,
                gitlab_id INTEGER NOT NULL,
                gitlab_login TEXT NOT NULL,
                gitlab_token TEXT NOT NULL,
                telegram_chat_id BIGINT NOT NULL 
            );
        """).format(table=sql.Identifier('users'))

        with self.conn.cursor() as cur:
            cur.execute(query)
        self.conn.commit()
        logging.info(f"Таблица users создана")

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            if not self.check_table_exists(DB_CONFIG['table_name']):
                self.create_users_table()
            else:
                logging.info(f"Таблица {DB_CONFIG['table_name']} существует")

            if not self.check_table_exists('tracked_issues'):
                self.create_tracked_issues_table()

            if not self.check_table_exists('issue_subscriptions'):
                self.create_issue_subscriptions_table()

            return self.conn
        except psycopg2.Error as e:
            logging.warning(f"Ошибка при выполнении запроса: {e}")
        return None

    def __exit__(self, type, value, traceback):
        if self.conn:
            if type:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()

    def get_user_by_telegram_id(self, telegram_id):
        try:
            with self as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT *  FROM users where telegram_id={telegram_id}")
                    row = cur.fetchone()
                    if row:
                        colnames = [desc[0] for desc in cur.description]
                        result = dict(zip(colnames, row))
                        logging.info(f"Пользователь найден {result}")
                        return result
                    else:
                        return None
        except psycopg2.Error as e:
            logging.warning(f"Ошибка при выполнении запроса: {e}")
            return None

    def get_user_by_gitlab_id(self, gitlab_id):
        try:
            with self as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT *  FROM users where gitlab_id={gitlab_id}")
                    row = cur.fetchone()
                    if row:
                        colnames = [desc[0] for desc in cur.description]
                        result = dict(zip(colnames, row))
                        logging.info(f"Пользователь найден {result}")
                        return result
                    else:
                        return None
        except psycopg2.Error as e:
            logging.warning(f"Ошибка при выполнении запроса: {e}")
            return None

    def create_user(self, telegram_id, gitlab_id='', gitlab_login='', gitlab_token='', telegram_chat_id=None):
        try:
            with self as conn:
                with conn.cursor() as cur:
                    query = f"""
                                INSERT INTO users (telegram_id, gitlab_id, gitlab_login, gitlab_token, telegram_chat_id)
                                VALUES (%s, %s, %s, %s, %s)
                            """
                    cur.execute(query, (telegram_id, gitlab_id, gitlab_login, gitlab_token, telegram_chat_id))
                    user_id = {
                        'telegram_id': telegram_id,
                        'gitlab_id': gitlab_id,
                        'gitlab_login': gitlab_login,
                        'gitlab_token': gitlab_token,
                        'telegram_chat_id': telegram_chat_id
                    }
                    logging.info(f"Пользователь создан {user_id}")
            return user_id
        except psycopg2.Error as e:
            logging.warning(f"Ошибка при выполнении запроса: {e}")
            return None

    def create_tracked_issues_table(self):
        """Создает таблицу для отслеживания созданных задач и последних комментариев."""
        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS tracked_issues (
                project_id    INTEGER    NOT NULL,
                issue_iid     INTEGER    NOT NULL,
                telegram_chat_id BIGINT  NOT NULL,
                notified      BOOLEAN    NOT NULL DEFAULT FALSE,
                last_note_id  INTEGER    NOT NULL DEFAULT 0,
                last_assignee_id INTEGER,
                PRIMARY KEY   (project_id, issue_iid)
            );
        """)
        with self.conn.cursor() as cur:
            cur.execute(query)
        self.conn.commit()

    def create_tracked_issue(self, project_id: int, issue_iid: int, telegram_chat_id: int):
        with self as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tracked_issues (project_id, issue_iid, telegram_chat_id, last_note_id) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT DO NOTHING",
                    (project_id, issue_iid, telegram_chat_id, 0)
                )

    def get_all_tracked_issues(self):
        """
        Возвращает все отслеживаемые задачи с последним комментарием.
        :return: List of tuples (project_id, issue_iid, telegram_chat_id, last_note_id)
        """
        with self as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT project_id, issue_iid, telegram_chat_id, last_note_id, last_assignee_id
                    FROM tracked_issues
                """)
                return cur.fetchall()

    def update_last_note_id(self, project_id: int, issue_iid: int, new_last_id: int):
        """
        Обновляет last_note_id для указанной задачи.
        """
        with self as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE tracked_issues
                    SET last_note_id = %s
                    WHERE project_id = %s AND issue_iid = %s
                """, (new_last_id, project_id, issue_iid))

    def add_subscription(self, telegram_id: int, project_id: int, issue_iid: int):
        with self as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO issue_subscriptions (user_telegram_id, project_id, issue_iid)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (telegram_id, project_id, issue_iid))

    def get_subscribers(self, project_id: int, issue_iid: int):
        with self as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT user_telegram_id FROM issue_subscriptions
                    WHERE project_id = %s AND issue_iid = %s;
                """, (project_id, issue_iid))
                return [row[0] for row in cur.fetchall()]

    def create_issue_subscriptions_table(self):
        """Создаёт таблицу для подписок на обновления по задачам."""
        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS issue_subscriptions (
                id SERIAL PRIMARY KEY,
                user_telegram_id BIGINT NOT NULL,
                project_id INTEGER NOT NULL,
                issue_iid INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_telegram_id, project_id, issue_iid)
            );
        """)
        with self.conn.cursor() as cur:
            cur.execute(query)
        self.conn.commit()
        logging.info("Таблица issue_subscriptions создана")

    def get_unnotified_issues(self):
        with self as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT project_id, issue_iid, telegram_chat_id
                      FROM tracked_issues
                     WHERE notified = FALSE;
                """)
                return cur.fetchall()

    def mark_issue_notified(self, project_id: int, issue_iid: int):
        with self as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE tracked_issues
                       SET notified = TRUE,
                           notified_at = NOW()
                     WHERE project_id = %s AND issue_iid = %s
                """, (project_id, issue_iid))

    def get_notified_unacked_older_than(self, cutoff: datetime.datetime):
        with self as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT project_id, issue_iid, telegram_chat_id
                      FROM tracked_issues
                     WHERE notified = TRUE
                       AND notified_at < %s
                """, (cutoff,))
                return cur.fetchall()

    def mark_issue_unnotified(self, project_id: int, issue_iid: int):
        with self as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE tracked_issues
                       SET notified = FALSE
                     WHERE project_id = %s
                       AND issue_iid = %s;
                """, (project_id, issue_iid))

    def delete_tracked_issue(self, project_id: int, issue_iid: int):
        with self as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM tracked_issues
                     WHERE project_id = %s AND issue_iid = %s;
                    """,
                    (project_id, issue_iid)
                )

    def update_last_assignee_id(self, project_id: int, issue_iid: int, assignee_id: int | None):
        with self as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE tracked_issues
                       SET last_assignee_id = %s
                     WHERE project_id = %s
                       AND issue_iid = %s
                """, (assignee_id, project_id, issue_iid))

db = Database(
    dbname=DB_CONFIG['dbname'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port']
)
