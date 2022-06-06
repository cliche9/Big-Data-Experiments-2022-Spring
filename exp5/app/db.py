from codecs import getdecoder
from mimetypes import init
import sqlite3

import click
from flask import current_app, g
# g(global): 全局对象, 存储当前db连接
from flask.cli import with_appcontext

def get_db():
    # 建立db连接
    if 'db' not in g:
        g.db = sqlite3.connect(
            current_app.config['DATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        # 设置连接以行为单位返回查询数据
        g.db.row_factory = sqlite3.Row
    
    return g.db

def close_db(e=None):
    # 关闭db连接
    db = g.pop('db', None)

    if db is not None:
        db.close()

def init_db():
    db = get_db()
    
    with current_app.open_resource('schema.sql') as f:
        db.executescript(f.read().decode('utf8'))

# 定义名为'init-db'的命令行
@click.command('init-db')
@with_appcontext
def init_db_command():
    """清除原有数据, 建立新表"""
    init_db()
    click.echo('Initialized the database.')

def init_app(app):
    # 设置清理时运行close_db()
    app.teardown_appcontext(close_db)
    # 添加命令行
    app.cli.add_command(init_db_command)

