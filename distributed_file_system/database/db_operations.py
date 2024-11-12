import sqlite3
import os
import json
from datetime import datetime

# Database file location
DB_FILE = 'database/dfs_metadata.db'

# Ensure the database directory exists
if not os.path.exists('database'):
    os.makedirs('database')

def init_db():
    """
    Initializes the SQLite database and creates required tables if they don't exist.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create the files table
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
    conn.close()


def add_file_record(file_id, file_name, chunks_info):
    """
    Adds a new file record to the database.

    Args:
        file_id (str): Unique ID of the file.
        file_name (str): Name of the file.
        chunks_info (list): Metadata about file chunks.
    """
    chunks_json = json.dumps(chunks_info)
    created_at = datetime.now().isoformat()

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO files (id, name, chunks, created_at)
        VALUES (?, ?, ?, ?)
    ''', (file_id, file_name, chunks_json, created_at))

    conn.commit()
    conn.close()


def get_all_files():
    """
    Retrieves all files from the database, excluding soft-deleted files.

    Returns:
        list: List of file metadata.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, name, chunks, created_at, updated_at
        FROM files
        WHERE deleted_at IS NULL
    ''')

    files = cursor.fetchall()
    conn.close()

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

    Args:
        file_id (str): Unique ID of the file.

    Returns:
        dict: File metadata, or None if not found or soft-deleted.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, name, chunks, created_at, updated_at
        FROM files
        WHERE id = ? AND deleted_at IS NULL
    ''', (file_id,))

    row = cursor.fetchone()
    conn.close()

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

    Args:
        file_id (str): Unique ID of the file.
    """
    print(file_id,"file Id")
    deleted_at = datetime.now().isoformat()

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE files
        SET deleted_at = ?
        WHERE id = ?
    ''', (deleted_at, file_id))

    conn.commit()
    conn.close()
