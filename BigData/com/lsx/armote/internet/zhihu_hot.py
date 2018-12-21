#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : zhihu_hot.py
# @Author: Armote
# @Date  : 2018/12/21 0021
# @Desc  : 爬取知乎热搜

from urllib import request
from bs4 import BeautifulSoup
import re
import string
import json
import datetime
from com.lsx.armote.dataBase.db_server import DBServer

def visitUrl(url):
    """访问网页获取数据"""
    #需要加入cookie
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
        'Method': 'GET',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Cookie':'_zap=4e901c71-5f5e-4445-8ead-532a407e4799; _xsrf=bsreoWIfAaVV5HsT64sH0FZrJMTmxcDC; d_c0="AKBif_h8sg6PTjV0-9TZ_U4mordk8zGTYes=|1545276306"; capsion_ticket="2|1:0|10:1545276353|14:capsion_ticket|44:ZGU0YTk1Mjc1MGZjNDRmM2E1ODhiZTdjYjk1MDFkNjY=|091db42d81865777eea46702144693353fb0d16ef42860ba69285afed4a5aa1c"; z_c0="2|1:0|10:1545276429|4:z_c0|92:Mi4xSHAzcEF3QUFBQUFBb0dKXy1IeXlEaVlBQUFCZ0FsVk5EVm9JWFFBX0VCRWZvUHRQUEk1YVJaQWpDWjdTaVZ6dnFB|fe8a4b13f65772ca2508e4c1badbef9f7309c7fd2d3ca8b56f5435b6b4493faf"; q_c1=d6a1e0882a31410981f2c431c0dae5f9|1545276444000|1545276444000; __gads=ID=87d14ea2c44ae0ff:T=1545360247:S=ALNI_MaZHV428d24rRhDaLLo6X3vbsa47w; tgw_l7_route=ec452307db92a7f0fdb158e41da8e5d8; __utma=51854390.1092382053.1545361106.1545361106.1545361106.1; __utmb=51854390.0.10.1545361106; __utmc=51854390; __utmz=51854390.1545361106.1.1.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/search; __utmv=51854390.100--|2=registration_date=20170109=1^3=entry_date=20170109=1; tst=h'
    }
    res = request.Request(url, headers=header)
    html = request.urlopen(res)
    bsObj = BeautifulSoup(html)
    return bsObj

def dataClean(context):
    """数据清洗"""
    #list数据 以<section>包围一个话题 class=HotItem
    conversionList = []
    for section in context.findAll('section', {'class': 'HotItem'}):
        # 排名div-class:HotItem-rank HotItem-hot
        hotTop = section.find('div', {'class': re.compile('HotItem-rank*')}).get_text()
        # 标题h2-class:HotItem-title
        hotTitle = section.find('h2',{'class':'HotItem-title'}).get_text()
        # 热度 svg-class:Zi Zi--Hot
        hotFire = section.find('div', {'class': re.compile('HotItem-metrics*')}).get_text()
        # 链接 div-class:HotItem-content>a-href
        hotUrl = section.find('div', {'class': re.compile('HotItem-content*')}).find('a').attrs['href']
        #print('排名:'+str(hotTop)+',热度:'+hotFire+',链接:'+hotUrl+',标题:'+hotTitle)
        zhihu = {}
        zhihu['hot_top'] = hotTop
        zhihu['hot_title'] = hotTitle
        zhihu['hot_fire'] = hotFire
        zhihu['hot_url'] = hotUrl
        zhihu['etl_date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        zhihu['source'] = 'https://www.zhihu.com/hot'
        conversionList.append(zhihu)
    print('数据转换完毕:'+str(conversionList))
    return conversionList

def saveDB(db,conversionList):
    """保存数据库"""
    tablename = "lsx_zhihu_hot"
    columns = "hot_top,hot_title,hot_fire,hot_url,etl_date,source"
    for data in conversionList:
        param =(data['hot_top'],data['hot_title'],data['hot_fire'],data['hot_url'],data['etl_date'],data['source'])
        sql = "insert into "+tablename+"(" + columns + ")values('%s','%s','%s','%s','%s','%s')" % param
        db.insertOne(sql)
    #全部保存后关闭连接
    db.closeDataBase()

url = 'https://www.zhihu.com/hot'
context = visitUrl(url)
conversionList = dataClean(context)
db = DBServer()
saveDB(db,conversionList)
#db.selectData('lsx_zhihu')
