import imdb
import csv

from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from sqlalchemy import null
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
    movie = dict(
        db.execute(
            'SELECT movies.*, links.imdbId'
            ' FROM movies JOIN links'
            ' WHERE title LIKE ?',
            ('%' + name + '%', )
        ).fetchone()
    )

    if movie is None:
        abort(404, f"Movie《{name}》doesn't exist.")
    
    # 查找推荐的电影
    related_movies = null
    #db.execute(
        
    #).fetchall()
    movie['relatives'] = related_movies

    return movie

# 详细界面
@bp.route('/details', methods=('GET', 'POST'))
def details():
    if request.method == 'POST':
        movie_info = get_movie(request.form['movie_name'])
        return render_template('movie/details.html', movie_info=movie_info)
    elif request.method == 'GET':
        movie_info = get_movie(request)
        return render_template('movie/details.html', movie_info=movie_info)