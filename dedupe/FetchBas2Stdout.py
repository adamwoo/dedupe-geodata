#!/usr/bin/python
#coding=utf8

"""
@:param 在FetchBas2Csv.yml中配置：包括:数据库参数,表名称
@:creator wuyudong
@:description 读取当天的聚源和贝格数据表中终止上市的数据，取出两表数据中国不同的部分打印在控制台上。
@:return

********what need to install************
pip install cx_Oracle
pip install yaml

***********how to run*******************
1. please chmod  +x  FetchBas2Csv.py
2. crontab -e
    0 8 * * * root run-parts /etc/FetchBas2Csv.py(script dictionary)
   :wq
3. service cron restart
"""

import pandas as pd
import cx_Oracle as oracle
import pymysql
import yaml
import datetime
import requests,json
import logging
from logging import handlers
import os


# 设置日志路径和日志位置以及格式
def getlogger():
    log_dir = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    file_handler = logging.handlers.RotatingFileHandler("./logs/test.log", mode="w", maxBytes=10000000, backupCount=10, encoding="utf-8")
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(module)s\t%(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


logger = getlogger()


# 加载配置文件
def loadconfig():
    try:
        f = open('FetchBas2Stdout_Config.yml', 'r', encoding='utf8')
        cfg = f.read()
        conf = yaml.load(cfg, Loader=yaml.FullLoader)
        f.close()
        return conf
    except Exception as e:
        logger.error('configs error ****** 配置文件读取出错: '+str(e))


global search_time
# now=datetime.datetime.now()
# todo 修改起止时间,eg:
# starttime=(now-datetime.timedelta(days=10)).strftime('%Y-%m-%d 00:00:00')
# endtime=(now+datetime.timedelta(days=10)).strftime('%Y-%m-%d 23:59:59')
# search_time=now.strftime('%Y-%m-%d')
search_time = '2019-07-25'


# 当天终止上市数据获取
def oraclesql(cursor, table):
    # 本地测试SQL：
    # fp_sql="SELECT SECU_ID AS TRD_CODE FROM "+ table+" A  WHERE  TO_CHAR(A.CHG_DT,'YYYY-mm-dd')='"+search_time+"' AND A.STS_CODE = 3"
    # modified by wuyd @2019-07-22 10:32 @comment:获取证券ID trd_code 用于结果展示
    # fp_sql="SELECT A.* FROM "+ table+" A JOIN BAS_SECU_INFO B ON A.SECU_ID=B.SECU_ID WHERE B.TYP_CODEI = 11 AND TO_CHAR(A.CHG_DT,'YYYY-mm-dd')='"+search_time+"' AND A.STS_CODE = 3"
    fp_sql = "SELECT B.TRD_CODE AS TRD_CODE FROM " + table + " A JOIN BAS_SECU_INFO B ON A.SECU_ID=B.SECU_ID WHERE B.TYP_CODEI = 11 AND TO_CHAR(A.CHG_DT,'YYYY-mm-dd')='" + search_time + "' AND A.STS_CODE = 3"
    cursor.execute(fp_sql)
    return [x[0] for x in cursor.description], cursor.fetchall()


# 东方财富： 当天终止上市数据获取
def mysql_sql(cursor, table):
    # now = datetime.datetime.now()
    # search_time2 = now.strftime('%Y-%m-%d')
    search_time2 = search_time
    fp_sql = "SELECT SECURITYCODE AS TRD_CODE FROM " + table+"   WHERE  DATE_FORMAT(ENDDATE,'%Y-%m-%d')='" + search_time2 + "'"
    cursor.execute(fp_sql)
    return [x[0] for x in cursor.description], cursor.fetchall()


# 数据处理方法区
def df_operation(config):
    # 初始化列名和数据名
    jy_cols = []
    bg_cols = []
    dc_cols = []
    jy_data = []
    bg_data = []
    dc_data = []
    inconsist_data = pd.DataFrame()
    consist_data = pd.DataFrame()
    # oracle数据获取
    try:
        # Oracle 数据库获取数据
        print(str(config['oracle']['username'])+"/"+str(config['oracle']['password'])+"@"+str(config['oracle']['ipaddr'])+":"+str(config['oracle']['oracle_port'])+"/"+str(config['oracle']['oracle_service']))
        oracle_conn = oracle.connect(str(config['oracle']['username'])+"/"+str(config['oracle']['password'])+"@"+str(config['oracle']['ipaddr'])+":"+str(config['oracle']['oracle_port'])+"/"+str(config['oracle']['oracle_service']))
        print(str(config['oracle']['username'])+"/"+str(config['oracle']['password'])+"@"+str(config['oracle']['ipaddr'])+":"+str(config['oracle']['oracle_port'])+"/"+str(config['oracle']['oracle_service']))
        oracle_cursor = oracle_conn.cursor()
        # jy：聚源,bg:贝格
        jy_cols, jy_data = oraclesql(oracle_cursor, config['oracle']['jy_table_name'])
        bg_cols, bg_data = oraclesql(oracle_cursor, config['oracle']['bg_table_name'])
        oracle_cursor.close()
        oracle_conn.close()
    except Exception as e:
        logger.error("oracle数据库读取错误！详细错误原因："+str(e))
    # mysql 数据获取
    try:
        # MySQL 数据库获取数据
        mysql_conn = pymysql.connect(host=str(config['mysql']['host']), user=str(config['mysql']['user']), passwd=str(config['mysql']['password']), db=str(config['mysql']['database']))
        print(str(config['mysql']['host'])+str(config['mysql']['user'])+ str(config['mysql']['password'])+str(config['mysql']['database']))
        mysql_cursor = mysql_conn.cursor()
        # dc:东方财富
        dc_cols, dc_data = mysql_sql(mysql_cursor, config['mysql']['dc_table_name'])
        mysql_cursor.close()
        mysql_conn.close()
    except Exception as e:
        logger.error("mysql数据库读取错误！详细错误原因："+str(e))

    #  数据比对
    try:
        # 生成DataFrame
        res_set = {}
        if jy_data:
            jy = pd.DataFrame(list(jy_data), columns=jy_cols).drop_duplicates()
            jy['jy'] = 'exist'
            res_set['jy'] = jy
        if bg_data:
            bg = pd.DataFrame(list(bg_data), columns=bg_cols).drop_duplicates()
            bg['bg'] = 'exist'
            res_set['bg'] = bg
        if dc_data:
            dc = pd.DataFrame(list(dc_data), columns=dc_cols).drop_duplicates()
            dc['dc'] = 'exist'
            res_set['dc'] = dc
        # 判断是否全为空
        if res_set:
            df_list = list(res_set.keys())
            tmp_result = res_set[df_list[0]]
            for key in range(1, len(df_list)):
                tmp_result = pd.merge(tmp_result, res_set[df_list[key]], how='outer')
            consist_data = tmp_result.dropna(how='any')
            inconsist_data = tmp_result[~tmp_result['TRD_CODE'].isin(list(consist_data['TRD_CODE']))]
            inconsist_data.fillna('not exist', inplace=True)
        # else:
        #     return "没有数据"
        # result=pd.merge(jy,bg,how='outer')
        # result=pd.merge(result,dc,how='outer')
        # clear_data=result[(result['jy']=='exist') & (result['bg']=='exist') & (result['dc']=='exist')]
        # result=result[(result['jy']!='exist') | (result['bg']!='exist') | (result['dc']!='exist')]
        # result.fillna('not exist',inplace=True)
        # 数据比对
        # result=result.drop_duplicates(subset=['SECU_ID','CHG_DT','STS_CODE'])
        # modified by wuyd@20190719-16:43
        #result=result.drop_duplicates(subset=['SECU_ID','CHG_DT','STS_CODE'], keep=False)
    except Exception as e:
        logger.exception("其他错误："+str(e))
        return "其他错误"
    cnt = 0
    words_dict = {
        'jy': "聚源数据库基金",
        'bg': "贝格数据库基金",
        'dc': "东方财富数据库基金"
    }
    # 结果输出
    print("*************"+search_time+": 聚源&贝格&东方财富数据比对***********")
    if not consist_data.empty:
        print("****************多家数据一致区域******************")
        columns = list(consist_data.columns)
        columns.remove("TRD_CODE")
        for index, line in consist_data.iterrows():
            template = "事件编号241和249:\n"
            flag = 0
            for column in columns:
                if not flag:
                    template = template + words_dict[column] + "[" + str(line['TRD_CODE']) + "]"
                    flag = 1
                else:
                    template = template + '\n' + words_dict[column] + "[" + str(line['TRD_CODE']) + "]"
            print(template)
            print("------------------------------------------")
    if not inconsist_data.empty:
        print("****************多家数据不一致区域******************")
        columns = list(consist_data.columns)
        columns.remove("TRD_CODE")
        for index, line in inconsist_data.iterrows():
            cnt += 1
            flag = 0
            for column in columns:
                if not flag:
                    template = template + words_dict[column] + "[" + str(line['TRD_CODE']) + "]" + str(line[column])
                    flag = 1
                else:
                    template = template + '\n' + words_dict[column] + "[" + str(line['TRD_CODE']) + "]" + str(line[column])
            print(template)
            print("------------------------------------------")

    if cnt == 0:
        print(search_time+"：数据全部一致")
    else:
        inconsist_dict = {
            "jy": "聚源包含记录",
            "bg": "贝格包含记录",
            "dc": "东方财富包含记录"
        }
        template = search_time+"：不一致的记录共有"+str(cnt)+"条 : \n"
        columns = list(consist_data.columns)
        columns.remove("TRD_CODE")
        for column in columns:
            template += template + inconsist_dict[column] + str(len(inconsist_data[inconsist_data[column]=='exist']))+"条;\n"
        print(template)


# 发送POST请求到微信端接口
def send_wechat(configs, data):
    try:
        with open(configs['json-template'], 'r', encoding='utf-8-sig') as f:
            template = json.load(f)
    except Exception as e:
        logger.error("json模板文件不存在，发送失败！错误详情："+str(e))
        return

    template['content'] = data
    try:
        r = requests.post(configs['wechat_url'], json=template)
        if r:
            print("发送成功")
    except Exception as e:
        logger.error("请求发送失败，请检查接口名是否正确，以及连接是否可用。错误详情："+str(e))


if __name__ == "__main__":
    all_config = loadconfig()
    # DataFrame 实现
    df_operation(all_config)
    #send_wechat(all_config,"hello world!")

