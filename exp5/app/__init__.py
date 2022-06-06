"""
1. 包含应用工厂
2. 告诉python: app应视为包
"""
import os

from flask import Flask

def create_app(test_config=None):
    # 创建/设置 app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'movie.sqlite')
    )

    if test_config is None:
        # 非测试模式下导入设置
        app.config.from_pyfile('config.py', silent=True)
    else:
        # 导入测试设置
        app.config.from_mapping(test_config)    

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/hello')
    def hello():
        return 'Hello, World!'

    from . import db, auth, movie
    # app注册数据库初始化函数
    db.init_app(app)
    # app注册用户auth蓝图
    # app.register_blueprint(auth.bp)
    # app注册电影movie蓝图
    app.register_blueprint(movie.bp)
    app.add_url_rule('/', endpoint='index')

    return app