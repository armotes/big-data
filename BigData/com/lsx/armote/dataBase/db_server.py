#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : db_server.py
# @Author: Armote
# @Date  : 2018/12/20 0020
# @Desc  : 数据库连接服务
import pymysql
class DBServer():
    """数据库连接服务"""

    def __init__(self):
        """初始化连接数据：并且启动数据库连接:使用后记得关闭连接"""
        self.host = '127.0.0.1'
        self.port = 3306
        self.user = 'root'
        self.password = '4130'
        self.db ='lsx'

        #开始连接数据库
        conn = pymysql.connect(host=self.host, port=self.port, user=self.user,
                               password=self.password, db=self.db, charset='utf8')
        print('当前操作:MysqlDB()实例化完毕,数据库开启成功!')
        cur = conn.cursor()
        self.cur = cur
        self.conn =conn

    def connDataBase(self):
        """连接数据库"""
        conn = pymysql.connect(host=self.host,port=self.port,user=self.user,password=self.password,db=self.db,charset='utf8')
        cur = conn.cursor()
        self.cur = cur
        self.conn = conn
        return cur

    def insertData(self,tablename,columns,values):
        """插入数据"""
        column = "tid,title,description,voteup_count,comment_count,url,etl_date,source"
        sql = "insert into lsx_zhihu(" + column + ") values('%d','%s','%s','%d','%d','%s','%s','%s');"
        try:
            #参数传递:可以放在回调里面通过占位符赋值
            count = self.cur.execute(sql % (values['tid'], values['title'], values['description'], values['voteup_count'], values['comment_count'],values['url'], values['etl_date'], values['source']))
            print('插入数据数量:'+str(count))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
        finally:
            print(values)
            #self.conn.close()

    def insertOne(self,sql):
        """单独条数添加,自定义封装sql语句和参数"""
        try:
            # 参数传递:可以放在回调里面通过占位符赋值
            count = self.cur.execute(sql)
            print('插入数据数量:' + str(count))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
        finally:
            print(sql)
            # self.conn.close()



    def selectData(self,tablename):
        """查询数据"""
        sql = 'SELECT * FROM '+tablename
        cur = self.cur
        try:
            cur.execute(sql)
            resultList = cur.fetchall()
            if resultList != None:
                for row in resultList:
                    #id = row[0]
                    #name = row[3]
                    print(row)
        except Exception as e:
            raise e
        finally:
            self.conn.close()



    def closeDataBase(self):
        """关闭数据库"""
        self.cur.close()
        self.conn.close()
        print('当前操作:数据库连接已关闭!')


