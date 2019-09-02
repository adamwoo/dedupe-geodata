#! /usr/bin/python3
# -*- coding: utf-8 -*-
from flask import Flask
import logging
import yaml


def get_logger():
    local_logger = logging.getLogger()
    local_logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(filename='D:\wyd\dedupe_test1\dedupe.log', encoding='utf8')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    local_logger.addHandler(fh)
    return local_logger


logger = get_logger()


def load_config(path):
    try:
        with open(path, mode='r', encoding='utf8') as f:
            cfg = f.read()
            local_config = yaml.load(cfg)
            logger.info("info:%s", "配置信息加载完成："+str(local_config))
            return local_config
    except Exception as e:
        logger.exception("error:%s", str(e))
        return


config = load_config(b"D:\wyd\dedupe_test1\_config.yml")


app = Flask(__name__)


@app.route('/test', methods=["GET"])
def hello_world():
    logger.info("info:%s", "the port runs ok")
    return 'Hello World!'


if __name__ == '__main__':
    app.debug = True
    app.run(host="0.0.0.0", port=9000)
