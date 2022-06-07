from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from werkzeug.exceptions import abort

from app.db import get_db

bp = Blueprint('movie', __name__)

# 主页面
@bp.route('/')
def index():
    return render_template('movie/index.html')

# 获取对应name的电影页面
def get_movie(name):
    db = get_db()
    # 查找电影
    movie = db.execute(
        'SELECT p.id, title, body, created, author_id, username'
        ' FROM post p JOIN user u ON p.author_id = u.id'
        ' WHERE p.id = ?',
        (name, )
    ).fetchone()

    if movie is None:
        abort(404, f"Movie《{name}》doesn't exist.")
    
    # 查找推荐的电影
    related_movies = db.execute(
        
    ).fetchall()

    return movie, related_movies

# 查找movie
@bp.route('/search', methods=('POST',))
def search(name):
    movie, related_movies = get_movie(name)
    return render_template('movie/details.html', movie=movie, related_movies=related_movies)

# 主页面
@bp.route('/details', methods=('GET', 'POST'))
def details():
    return render_template('movie/details.html')