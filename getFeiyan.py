import time
import requests
import json
import pymysql
import traceback

url="https://view.inews.qq.com/g2/getOnsInfo?name=disease_h5"
#https://view.inews.qq.com/g2/getOnsInfo?name=disease_h5
#https://view.inews.qq.com/g2/getOnsInfo?name=disease_other
headers={
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36"
}
res=requests.get(url,headers)
#将获取的json数据转换成对应的字典
r=json.loads(res.text)
data_a=json.loads(r["data"])


#pymysql的简单使用
#建立连接
conn=pymysql.connect(host="localhost",user="root",password="926434",db="cov")
print(conn)
#创建游标,默认是元组类型
cursor=conn.cursor()
sql="select *from history"
cursor.execute(sql)
#conn.commit() #提交事务
res=cursor.fetchall()
print(res)
cursor.close()
conn.close()

def get_conn():
    #创建连接
    conn=pymysql.connect(host="localhost",user="root",password="926434",db="cov")
    cursor=conn.cursor()
    return conn,cursor

def close_conn(conn,cursor):
    if cursor:
        cursor.close()
    if conn:
        conn.close()


def get_tencent_data():
    url = "https://view.inews.qq.com/g2/getOnsInfo?name=disease_other"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36"
    }
    res = requests.get(url, headers)
    # 将获取的json数据转换成对应的字典
    r = json.loads(res.text)
    data_all = json.loads(r["data"])

    history = {}  # 历史数据
    for i in data_all["chinaDayList"]:
        ds = "2020." + i["date"]
        tup = time.strptime(ds, "%Y.%m.%d")
        ds = time.strftime("%Y-%m-%d", tup)
        confirm = i["confirm"]
        suspect = i["suspect"]
        heal = i["heal"]
        dead = i["dead"]
        history[ds] = {"confirm": confirm, "suspect": suspect, "heal": heal, "dead": dead}
    for i in data_all["chinaDayAddList"]:
        ds = "2020." + i["date"]
        tup = time.strptime(ds, "%Y.%m.%d")
        ds = time.strftime("%Y-%m-%d", tup)
        confirm = i["confirm"]
        suspect = i["suspect"]
        heal = i["heal"]
        dead = i["dead"]
        history[ds].update({"confirm_add": confirm, "suspect_add": suspect, "heal_add": heal, "dead_add": dead})

    details = []  # 当日详细数据
    update_time = data_a["lastUpdateTime"]
    data_country = data_a["areaTree"]  # list 25个国家
    data_province = data_country[0]["children"]
    for pro_infos in data_province:
        province = pro_infos["name"]  # 省名
        for city_infos in pro_infos["children"]:
            city = city_infos["name"]
            confirm = city_infos["total"]["confirm"]
            confirm_add = city_infos["total"]["confirm"]
            heal = city_infos["total"]["heal"]
            dead = city_infos["total"]["dead"]
            details.append([update_time, province, city, confirm, confirm_add, heal, dead])
    return history, details

def update_details():
    """更新details表"""
    cursor=None
    conn=None
    try:
        li=get_tencent_data()[1] #0是历史数据字典，1是最新详细数据列表
        conn,cursor=get_conn()
        sql="insert into details (update_time,province,city,confirm,confirm_add,heal,dead)values(%s,%s,%s,%s,%s,%s,%s)"
        sql_query="select %s=(select update_time from details order by id desc limit 1)"
        cursor.execute(sql_query,li[0][0])
        if not cursor.fetchone()[0]:
            print(f"{time.asctime()}开始更新最新数据")
            for item in li:
                cursor.execute(sql,item)
            conn.commit() # 提交事务 update delete insert 操作
            print(f"{time.asctime()}更新最新数据完毕！")
        else:
            print(f"{time.asctime()}已是最新数据！")
    except:
        traceback.print_exc()
    finally:
        close_conn(conn,cursor)

def insert_history():
    """插入历史数据"""
    cursor=None
    conn=None
    try:
        dic=get_tencent_data()[0]
        print(f"{time.asctime()}开始插入历史数据！")
        conn,cursor=get_conn()
        sql="insert into history values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for k,v in dic.items():
            # item格式{'2020-01-13':{'confirm':41,...}}
            cursor.execute(sql,[k,v.get("confirm"),v.get("confirm_add"),v.get("suspect"),
                               v.get("suspect_add"),v.get("heal"),v.get("heal_add"),
                                v.get("dead"),v.get("dead_add")])
        conn.commit()
        print(f"{time.asctime()}插入历史数据完毕！")
    except:
        traceback.print_exc()
    finally:
        close_conn(conn,cursor)

def update_history():
    '''更新历史数据'''
    ursor=None
    conn=None
    try:
        dic=get_tencent_data()[0]
        print(f"{time.asctime()}开始更新历史数据！")
        conn,cursor=get_conn()
        sql="insert into history values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql_query="select confirm from history where ds=%s"
        for k,v in dic.items():
            # item格式{'2020-01-13':{'confirm':41,...}}
            if not cursor.execute(sql_query,k):
                cursor.execute(sql_query,[k,v.get("confirm"),v.get("confirm_add"),v.get("suspect"),v.get("suspect_add"),v.get("heal"),v.get("heal_add"),v.get("dead"),v.get("dead_add")])
        conn.commit()
        print(f"{time.asctime()}历史数据更新完毕！")
    except:
        traceback.print_exc()
    finally:
        close_conn(conn,cursor)
