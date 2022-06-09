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

# 根据类别获取相似的电影
def find_sims_by_genres(movieId, db_movies, num):
    genres = db_movies.execute(
        '''
        SELECT genres FROM movies WHERE movieId = (?)
        ''',
        (movieId, )
    ).fetchone()[0].split('|')

    relatives = set(
        db_movies.execute(
            'SELECT movieId, title'
            ' FROM movies'
            ' WHERE genres LIKE (?)',
            ('%' + genres[0] + '%', )
        ).fetchall()
    )
    for keyword in genres[1:]:        
        movies = db_movies.execute(
            'SELECT movieId, title'
            ' FROM movies'
            ' WHERE genres LIKE (?)',
            ('%' + keyword + '%', )
        ).fetchall()
        next = relatives & set(movies)
        if len(next) < num:
            return list(relatives)[:num]
        relatives = next

    return list(relatives)[:num]

# 获取对应name的电影页面
def get_movie(name):
    db = get_db()
    # 查找电影
    movie = db.execute(
        'SELECT movies.*, links.imdbId'
        ' FROM movies JOIN links'
        ' WHERE title LIKE ?',
        ('%' + name + '%', )
    ).fetchone()

    if movie is None:
        abort(404, f"Movie《{name}》doesn't exist.")
    
    movie = dict(movie)
    # 查找推荐的电影
    movie['relatives'] = find_sims_by_genres(movie['movieId'], db, 3)

    return movie

# 详细界面
@bp.route('/details', methods=('GET', 'POST'))
def details():
    if request.method == 'POST':
        movie_info = get_movie(request.form['movie_name'])
        return render_template('movie/details.html', movie_info=movie_info)
    elif request.method == 'GET':
        movie_info = get_movie(request.args['movie_name'])
        return render_template('movie/details.html', movie_info=movie_info)