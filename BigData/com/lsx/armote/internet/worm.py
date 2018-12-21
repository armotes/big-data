#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : worm.py
# @Author: Armote
# @Date  : 2018/12/20 0020
# @Desc  : internet worm

from urllib import request
from bs4 import BeautifulSoup
import re
import string
import json
import datetime
from com.lsx.armote.dataBase.db_server import DBServer


def visitUrl(url):
    """访问网页获取数据"""
    userAgent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
    header = {
        'User-Agent': userAgent
    }
    res = request.Request(url, headers=header)
    html = request.urlopen(res)
   # html = urlopen(url);
    bsObj = BeautifulSoup(html)
    content = bsObj.findAll('body')
    #content = bsObj.findAll('span', {'class': 'Highlight'})
    #print(content)
    return content

def dataClean(context):
    """数据清洗"""
    context = str(context)
    #首先转化为json格式
    #len = len(context)
    context = context.replace('[<body>','')
    context = context.replace('</body>]', '')

    #转为初步JSON
    # jsonData = json.dumps(context) 字典转JSON
    jsonData = json.loads(context)
    #print(jsonData)

    #提取第一层JSON的data,并且转化为JSON:data是一个list字典，里面包含highlight和object和author等等嵌套对象
    dataInit = jsonData['data']
    dataList = list(dataInit)

    #conversion转换最终List:包含标题，描述，点赞数，评论数，url
    conversionList = []
    for data in dataList:
        title = data['highlight']['title']
        title = title.replace('<em>','')
        title = title.replace('</em>', '')

        description = data['highlight']['description']
        description = description.replace('<em>', '')
        description = description.replace('</em>', '')
        zhihu = {}
        zhihu['tid'] = int(data['object']['id'])
        zhihu['title'] = title
        zhihu['description'] = description
        zhihu['voteup_count'] = int(data['object']['voteup_count'])
        zhihu['comment_count'] =int(data['object']['comment_count'])
        zhihu['url'] = data['object']['url']
        zhihu['etl_date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        zhihu['source'] = 'https://www.zhihu.com'
        conversionList.append(zhihu)
    return conversionList

def saveDB(db,conversionList):
    """保存数据库"""
    columns = "tid,title,description,voteup_count,comment_count,url,etl_date,source"
    for data in conversionList:
        db.insertData('lsx_zhihu',columns,data)
    #全部保存后关闭连接
    db.closeDataBase()



url = 'https://www.zhihu.com/api/v4/search_v3?t=general&q=%E6%8E%92%E5%90%8D&correction=1&offset=110&limit=10&show_all_topics=0&search_hash_id=c1e63a6f3a5107987fd0c933216b965a&vertical_info=0%2C1%2C0%2C0%2C0%2C0%2C0%2C1%2C0%2C1'
#url = 'https://www.zhihu.com/search?type=content&q=%E6%8E%92%E5%90%8D'
context = visitUrl(url)
conversionList = dataClean(context)
db = DBServer()
saveDB(db,conversionList)
#db.selectData('lsx_zhihu')
columns = "tid,title,description,voteup_count,comment_count,url,etl_date,source"
#data={'tid':111,'title':'标题的啊','description':'描述什么的','voteup_count':10000,'comment_count':2000,'url':'www.baidu.con','etl_date':datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'source':'write'}
#db.insertData('lsx_zhihu',columns,data)

