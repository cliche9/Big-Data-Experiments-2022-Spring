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
        DATABASE=os.path.join(app.instance_path, 'app.sqlite')
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

    return app