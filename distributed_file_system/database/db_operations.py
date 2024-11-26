import sqlite3
import os
import threading
import json
from datetime import datetime

# Database file location
DB_FILE = 'database/dfs_metadata.db'

# Ensure the database directory exists
if not os.path.exists('database'):
    os.makedirs('database')

# Create a thread-local SQLite connection
db_local = threading.local()


def get_db_connection():
    """
    Returns a thread-local SQLite connection.
    """
    if not hasattr(db_local, "connection"):
        db_local.connection = sqlite3.connect(DB_FILE, check_same_thread=False)
    return db_local.connection


def init_db():
    """
    Initializes the SQLite database and creates required tables if they don't exist.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            chunks TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            deleted_at TEXT
        )
    ''')
    conn.commit()


def add_file_record(file_id, file_name, chunks_info):
    """
    Adds a new file record to the database.
    """
    chunks_json = json.dumps(chunks_info)
    created_at = datetime.now().isoformat()
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO files (id, name, chunks, created_at)
        VALUES (?, ?, ?, ?)
    ''', (file_id, file_name, chunks_json, created_at))
    conn.commit()

def get_all_files_with_deleted():
    """
    Retrieve all files from the database, including soft-deleted ones.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, name, chunks, created_at, updated_at, deleted_at
        FROM files
    ''')
    files = cursor.fetchall()
    return [
        {
            'id': row[0],
            'name': row[1],
            'chunks': json.loads(row[2]),
            'created_at': row[3],
            'updated_at': row[4],
            'deleted_at': row[5]
        }
        for row in files
    ]

def get_all_files():
    """
    Retrieves all files from the database, excluding soft-deleted files.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, name, chunks, created_at, updated_at
        FROM files
        WHERE deleted_at IS NULL
    ''')
    files = cursor.fetchall()
    return [
        {
            'id': row[0],
            'name': row[1],
            'chunks': json.loads(row[2]),
            'created_at': row[3],
            'updated_at': row[4]
        }
        for row in files
    ]


def get_file(file_id):
    """
    Retrieves a single file by its ID.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, name, chunks, created_at, updated_at
        FROM files
        WHERE id = ? AND deleted_at IS NULL
    ''', (file_id,))
    row = cursor.fetchone()

    if row:
        return {
            'id': row[0],
            'name': row[1],
            'chunks': json.loads(row[2]),
            'created_at': row[3],
            'updated_at': row[4]
        }
    return None


def soft_delete_file(file_id):
    """
    Marks a file as soft-deleted in the database.
    """
    deleted_at = datetime.now().isoformat()
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE files
        SET deleted_at = ?
        WHERE id = ?
    ''', (deleted_at, file_id))
    conn.commit()

def delete_file(file_id):
    """
    Permanently deletes a file record from the database.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        DELETE FROM files
        WHERE id = ?
    ''', (file_id,))
    conn.commit()