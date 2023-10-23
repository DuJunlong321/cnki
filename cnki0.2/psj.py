# _*_ coding : utf-8 _*_
# @Time : 2023/9/15 19:21
# @Author : djl
# @Project : requests
# @File : 爬数据
import datetime
import math
import os
import threading
import time
import traceback
import uuid
from random import choice
from queue import Queue, PriorityQueue

import mysql.connector
from lxml import etree
import requests
from selenium import webdriver
from urllib import parse
import json

# 创建一个锁，用于控制任务队列的访问
task_queue_lock = threading.Lock()
requester_lock = threading.Lock()
access_token_lock = threading.Lock()
log_lock = threading.Lock()
spider_and_task_lock = threading.Lock()


user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84",
    "Mozilla/5.0 (Android 11; Mobile; rv:91.0) Gecko/91.0 Firefox/91.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
    "Mozilla/5.0 (Linux; Android 12; Pixel 6 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.1000.0 Mobile Safari/537.36"
]

# 写数据库
resource_id = 0
def save_to_database(task_guid, maxpage):
    try:
        # 数据库配置
        connection = mysql.connector.connect(
            host='localhost',
            port='3306',
            user='root',
            password='123456',
            database='endotoxin'
        )

        cursor = connection.cursor()

        # 插入数据
        insert_query = "INSERT INTO t_resource (resource_id, resource_name, resource_type, author, publish_time, explanation, get_way, paper_url, created_by, created_time, checked, enlabled) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        # 从json里拿数据
        try:
            with open(f'{task_guid}/Bib.json', 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
            with open(f'{task_guid}/results.json', 'r', encoding='utf-8') as json_file:
                json_data2 = json.load(json_file)
        except Exception as e:
            print(f'存入数据库失败：{e}')
            return

        global resource_id  # 全局变量,编号唯一

        # 用你的数据替换以下示例数据
        for i in range(1, 20*maxpage+1):
            # data_to_insert = (resource_id, json_data[str(i)]['name'], json_data[str(i)]['db_type'], json_data[str(i)]['author'], json_data2[str(math.ceil(i/20))]['publish_time'], "Explanation", "Get Way", "Paper URL", 1, "2023-10-15 12:00:00", 1, 1)
            data_to_insert = (
            resource_id, json_data[str(i)]['name'], json_data[str(i)]['db_type'], json_data[str(i)]['author'],
            json_data2[str(math.ceil(i/20))][i-1]['publish_time'], json_data[str(i)]['abstract'], json_data[str(i)]['get_way'], json_data[str(i)]['url'], 1,
            json_data[str(i)]['crawl_time'], 1, 1)
            cursor.execute(insert_query, data_to_insert)

        # 提交更改
        connection.commit()

        # 关闭连接
        cursor.close()
        connection.close()
    except Exception as e:
        print(f'出错啦{e}')

# 获取检索首页
def get_results_text(page, keyword, SearchSql):
    # print("打标记：")
    # print(SearchSql)
    # 数据接口url
    url = 'https://kns.cnki.net/kns8/Brief/GetGridTableHtml'
    headers = {
        "User-Agent": choice(user_agents),
        'Origin': 'https://kns.cnki.net',
        'Cookie': 'Ecp_ClientId=c230613151300884073; knsLeftGroupSelectItem=1%3B2%3B; Ecp_ClientIp=112.36.83.191; Ecp_loginuserbk=dx1119; Hm_lvt_dcec09ba2227fd02c55623c1bb82776a=1692265001; SID_sug=128005; ASP.NET_SessionId=u0etj2ptwshup4u51kroevpw; eng_k55_id=015106; SID_kns_new=kns25128005; SID_kns8=015123152; CurrSortFieldType=desc; Ecp_showrealname=1; Ecp_lout=1; LID=; personsets=; pageReferrInSession=https%3A//chn.oversea.cnki.net/; firstEnterUrlInSession=https%3A//kns.cnki.net/kns8/DefaultResult/Index%3Fdbcode%3DSCDB%26crossDbcodes%3DCJFQ%252CCDMD%252CCIPD%252CCCND%252CCISD%252CSNAD%252CBDZK%252CCCJD%252CCCVD%252CCJFN%26korder%3DSU%26kw%3DAI; VisitorCapacity=1; CurrSortField=%e7%9b%b8%e5%85%b3%e5%ba%a6%2frelevant%2c(%e5%8f%91%e8%a1%a8%e6%97%b6%e9%97%b4%2c%27time%27)+desc; dblang=ch',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'text/html, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Length': '759',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'kns.cnki.net',
        'Pragma': 'no-cache',
        'Referer': 'https://kns.cnki.net/kns8/DefaultResult/Index?',
        'Sec-Ch-Ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    data = {
        "IsSearch": 'true' if page == 1 else 'false',
        "QueryJson": '{"Platform": "","DBCode": "CFLS","KuaKuCode": "CJFQ,CCND,CIPD,CDMD,BDZK,CISD,SNAD,CCJD,CJFN,CCVD","QNode": {"QGroup": [{"Key": "Subject", "Title": "", "Logic": 1, "Items": [{"Title": "主题", "Name": "SU", "Value": "'+keyword+'", "Operate": "%=", "BlurType": ""}], "ChildItems": []}]}, "CodeLang": "ch"}',
        "SearchSql": '' if page == 1 else str(SearchSql),
        "PageName": 'DefaultResult',
        # "HandlerId": '9',
        "DBCode": 'CFLS',
        "KuaKuCodes": 'CJFQ,CCND,CIPD,CDMD,BDZK,CISD,SNAD,CCJD,CJFN,CCVD' ,
        "CurPage": str(page),
        "RecordsCntPerPage": '20',
        "CurDisplayMode": 'listmode',
        "CurrSortField": '',
        "CurrSortFieldType": 'desc',
        "IsSortSearch": 'false',
        "IsSentenceSearch": 'false',
        "Subject": '' ,
    }
    # payload = urlencode(data)
    response = requests.post(url=url, headers=headers, data=data)
    page_text = response.text
    # print(page_text)
    if page == 1:
        tree = etree.HTML(page_text)
        SearchSql = tree.xpath('//input[@id="sqlVal"]/@value')
        return page_text, SearchSql[0]
    else:
        return page_text, SearchSql

# 解析检索首页
def parse_results_text(context,page, guid ):
    ##############################
    # # 定义多级目录的路径
    # directory_path = f'首页/{guid}'
    #
    # # 使用os.makedirs()创建多级目录
    # os.makedirs(directory_path, exist_ok=True)
    #
    # # 定义文件名及其路径
    # file_path = os.path.join(directory_path, f'{page}.html')
    # with open(file_path, 'w', encoding='utf-8') as f:
    #     f.write(context)
    ###############################
    tree = etree.HTML(context)
    # 序号
    index_list = tree.xpath("//td[@class='seq']/text()")
    # 题名
    name_list_labels = tree.xpath("//td[@class='name']/a[@class='fz14']")
    name_list = []
    for name_lable in name_list_labels:
        name = name_lable.xpath('normalize-space(.)')
        name_list.append(name)
    # 作者名
    author_list_labels = tree.xpath("//td[@class='author']")
    author_list = []
    for author_label in author_list_labels:
        author = author_label.xpath('normalize-space(.)')
        author_list.append(author)
    # 来源
    get_way_list_labels = tree.xpath("//td[@class='source']")
    get_way_list = []
    for get_way_label in get_way_list_labels:
        get_way = get_way_label.xpath('normalize-space(.)')
        get_way_list.append(get_way)

    # 发表时间
    publish_time_list = tree.xpath('//td[@class="date"]/text()')
    publish_time_list = [item.replace('\r', '').replace('\n', '').strip() for item in publish_time_list]
    # 数据库
    db_type_list = tree.xpath("//td[@class='data']/text()")
    db_type_list = [item.replace('\r', '').replace('\n', '').strip() for item in db_type_list]
    # 被引
    cited_list = tree.xpath("//td[@class='quote']/text()")
    cited_list = [item.replace('\r', '').replace('\n', '').strip() for item in cited_list]
    # 下载量
    download_list_lables = tree.xpath("//td[@class='download']")
    download_list = []
    for download_label in download_list_lables:
        download = download_label.xpath('normalize-space(.)')
        download_list.append(download)
    # 链接
    url_list = tree.xpath('//a[@class="fz14"]/@href')
    url_list = ['https://kns.cnki.net/' + item for item in url_list]
    # 爬取时间
    crawl_time_list = [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') for _ in range(20)]
    return index_list,name_list,author_list,get_way_list,publish_time_list,db_type_list,cited_list,download_list,url_list, crawl_time_list

# 保存检索结果json
# 检索结果列表
results = {}
def save_results(guid, page_num, index_list,name_list,author_list,get_way_list,publish_time_list,db_type_list,cited_list,download_list,url_list, crawl_time_list):
    # print('序号： ' + str(index_list))
    # print('题名: ' + str(name_list))
    # print('作者: ' + str(author_list))
    # print('来源: ' + str(get_way_list))
    # print('发表时间: ' + str(publish_time_list))
    # print('数据库: ' + str(db_type_list))
    # print('被引: ' + str(cited_list))
    # print('下载: ' + str(download_list))
    # print('url: ' + str(url_list))
    # print('爬取时间: ' + str(crawl_time_list))


    # 使用循环构建一组item JSON 结构
    for i, name in enumerate(name_list):
        item = {
            "index": str(index_list[i]),
            "name": name,
            "author": author_list[i],
            "get_way": get_way_list[i],
            "publish_time": publish_time_list[i],
            "db_type": db_type_list[i],
            "cited": cited_list[i],
            "download": download_list[i],
            "url": url_list[i],
            "crawl_time": crawl_time_list[i]
        }
        global results
        if page_num not in results:
            results[page_num] = []
        results[page_num].append(item)

    # 使用 json.dump() 函数将数据保存到文件中
    with open(f'{str(guid)}/results.json', 'w', encoding='utf-8', ) as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=4) # ensure_ascii=False确保 JSON 数据中的非 ASCII 字符以它们的原始形式进行保存，而不是进行 Unicode 转义


# 获取文献题录首页
def get_Bib_text(url):
    headers = {
        # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        # 'Accept-Encoding': 'gzip, deflate, br',
        # 'Accept-Language': 'zh-CN,zh;q=0.9',
        # 'Cache-Control': 'no-cache',
        # 'Connection': 'keep-alive',
        # # 'Cookie': 'Ecp_notFirstLogin=k2FzC8; Ecp_ClientId=c230613151300884073; knsLeftGroupSelectItem=1%3B2%3B; Ecp_ClientIp=112.36.83.191; Ecp_loginuserbk=dx1119; ASP.NET_SessionId=3xtys2zs11qzln4xfubb1kxn; SID_kns8=25123162; SID_kns_new=kns25128008; Ecp_session=1; cangjieStatus_NZKPT2=true; cangjieConfig_NZKPT2=%7B%22status%22%3Atrue%2C%22startTime%22%3A%222022-10-20%22%2C%22endTime%22%3A%222030-12-31%22%2C%22orginHosts%22%3A%22kns.cnki.net%22%2C%22type%22%3A%22mix%22%2C%22poolSize%22%3A%2210%22%2C%22intervalTime%22%3A10000%2C%22persist%22%3Afalse%7D; SID_sug=128005; CurrSortFieldType=desc; eng_k55_id=015106; CurrSortField=%e7%9b%b8%e5%85%b3%e5%ba%a6%2frelevant%2c(%e5%8f%91%e8%a1%a8%e6%97%b6%e9%97%b4%2c%27time%27)+desc; dblang=ch; language=chs; language=chs; LID=WEEvREcwSlJHSldSdmVpbisvR0MxdlFRdEg1TGVQQ2Naa2QvU2JjQ3Q3dz0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4IQMovwHtwkF4VYPoHbKxJw!!; Ecp_LoginStuts={"IsAutoLogin":false,"UserName":"dx1119","ShowName":"%E6%B5%8E%E5%8D%97%E5%A4%A7%E5%AD%A6","UserType":"bk","BUserName":"","BShowName":"","BUserType":"","r":"k2FzC8","Members":[]}; Hm_lvt_dcec09ba2227fd02c55623c1bb82776a=1694864125; Hm_lpvt_dcec09ba2227fd02c55623c1bb82776a=1694864125; c_m_LinID=LinID=WEEvREcwSlJHSldSdmVpbisvR0MxdlFRdEg1TGVQQ2Naa2QvU2JjQ3Q3dz0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4IQMovwHtwkF4VYPoHbKxJw!!&ot=09%2F16%2F2023%2019%3A47%3A18; c_m_expire=2023-09-16%2019%3A47%3A18',
        # 'Host': 'kns.cnki.net',
        # 'Pragma': 'no-cache',
        # 'Referer': 'https://kns.cnki.net/kns8/DefaultResult/Index?dbcode=CFLS',
        # 'Sec-Ch-Ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        # 'Sec-Ch-Ua-Mobile': '?0',
        # 'Sec-Ch-Ua-Platform': 'Windows',
        # 'Sec-Fetch-Dest': 'document',
        # 'Sec-Fetch-Mode': 'navigate',
        # 'Sec-Fetch-Site': 'same-origin',
        # 'Sec-Fetch-User': '?1',
        # 'Upgrade-Insecure-Requests': '1',
        'User-Agent': choice(user_agents)
    }
    response = requests.get(url=url, headers=headers)
    page_text = response.text
    return page_text

# 解析文献题录
def parse_Bib_text(content, affilation_list, page_list, pageNum_list, abstract_list, keywords_list, publish_time_list):
    tree = etree.HTML(content)

    # 隶属单位
    affilation_data = tree.xpath("//h3[2]//a[@class='author']/text()")
    affilation_data = [item.replace('\r', '').replace('\n', '').strip() for item in affilation_data]
    affilation_list.append(affilation_data)

    # 页码
    page_data = tree.xpath("//p[@class='total-inform']/span[1]/text()")
    page_data = [item.replace('\r', '').replace('\n', '').strip() for item in page_data]
    page_list.append(page_data)

    # 页数
    pageNum_data = tree.xpath("//p[@class='total-inform']/span[2]/text()")
    pageNum_data = [item.replace('\r', '').replace('\n', '').strip() for item in pageNum_data]
    pageNum_list.append(pageNum_data)

    # 摘要
    abstract_data = tree.xpath("//span[@id='ChDivSummary']/text()")
    abstract_data = [item.replace('\r', '').replace('\n', '').strip() for item in abstract_data]
    abstract_list.append(abstract_data)

    # 关键词
    keywords_data = tree.xpath("//p[@class='keywords']//a/text()")
    keywords_data = [item.replace('\r', '').replace('\n', '').strip() for item in keywords_data]
    keywords_list.append(keywords_data)

    # 发表时间
    publish_time_data = tree.xpath("//div[@class='top-tip']/span//a/text()")
    publish_time_data = [item.replace('\r', '').replace('\n', '').strip() for item in publish_time_data]
    publish_time_list.append(publish_time_data)

    # print(publish_time_list)  # bug
    # print('题名: ' + str(name_list))
    # print("隶属单位： " + str(affilation_list))
    # print("页码： " + str(page_list))
    # print("页数： " + str(pageNum_list))
    # print("摘要： " + str(abstract_list))
    # print("关键词： " + str(keywords_list))
# 保存文献题录json
# 文献题录信息

# 如果没有成功，也要进行赋值，避免后续产生错误
def parse_Bib_text_error(affilation_list, page_list, pageNum_list, abstract_list, keywords_list, publish_time_list):
   for i in range(0, 21):
       affilation_list[i] = ''
       page_list[i] = ''
       pageNum_list[i] = ''
       abstract_list[i] = ''
       keywords_list[i] = ''
       publish_time_list[i] = ''


Bib = {}
def save_Bib(guid, index_list, name_list, author_list, affilation_list, get_way_list, page_list, pageNum_list, publish_time_list,
             db_type_list, cited_list, download_list, abstract_list, keywords_list, url_list, crawl_time_list):
    # print('题名: ' + str(name_list))
    # print("隶属单位： " + str(affilation_list))
    # print("页码： " + str(page_list))
    # print("页数： " + str(pageNum_list))
    # print("摘要： " + str(abstract_list))
    # print("关键词： " + str(keywords_list))

    # 使用循环构建一组item JSON 结构
    for i, index in enumerate(index_list):
        item = {
            "name": name_list[i],
            "author": author_list[i],
            "affiliation": affilation_list[i][0] if len(affilation_list[i])>0 else '',
            "get_way": get_way_list[i] ,
            "page":  page_list[i][0] if len(page_list[i])>0 else '',
            "pageNum":  pageNum_list[i][0] if len(pageNum_list[i])>0 else '',
            "publish_time": publish_time_list[i],
            "db_type": db_type_list[i],
            "cited": cited_list[i],
            "download": download_list[i],
            "abstract": abstract_list[i][0] if len(abstract_list[i])>0 else '',
            "keywords": keywords_list[i][0] if len(keywords_list[i])>0 else '',
            "url": url_list[i],
            "crawl_time": crawl_time_list[i]
        }
        global Bib
        Bib[str(index_list[i])]=(item)

        # 使用 json.dump() 函数将数据保存到文件中
        with open(f'{str(guid)}/Bib.json', 'w', encoding='utf-8', ) as json_file:
            json.dump(Bib, json_file, ensure_ascii=False,
                        indent=4)  # ensure_ascii=False确保 JSON 数据中的非 ASCII 字符以它们的原始形式进行保存，而不是进行 Unicode 转义

# 保存:task.json 相当于爬虫本地的任务的总表
#     spider.json 记录任务执行状态信息
spider = {}
def save_spider_and_task(guid='', keywords='',requester='', task_type='', first_at='', finish_at='',  state='', success='0',  fail='0', fail_list='', maxpage=''):
    spider = {
         "guid": str(guid),
        "keywords": str(keywords),
        "requester": str(requester),
        "task_type": str(task_type),
        "first_at": str(first_at),
        "finish_at": str(finish_at),
        "state": str(state),
        "success": str(success),
        "fail": str(fail),
        "fail_list": str(fail_list),
        "maxpage": str(maxpage),
    }
    # 尝试打开现有的JSON文件，如果文件不存在，创建一个新的空字典
    try:
        with open('task.json', "r", encoding='utf-8') as json_file:
            task = json.load(json_file)
    except FileNotFoundError:
        task = {}
    # 向现有数据中添加新的键值对
    task[str(guid)] = spider
    # 存入数据
    with open('task.json', 'w', encoding='utf-8') as json_file:
        json.dump(task, json_file, ensure_ascii=False,
                  indent=4)
    # 使用 json.dump() 函数将数据保存到文件中
    with open(f'{str(guid)}/spider.json', 'w', encoding='utf-8', ) as json_file:
        json.dump(spider, json_file, ensure_ascii=False,
                  indent=4)


# 爬取数据
def crawl(guid, keywords, requester, task_type, maxpage=50):

    first_at = datetime.datetime.now()  # 记录任务开始时间
    success = 0 # 成功爬取的条数
    fail = 0    # 失败爬取的条数
    fail_list = [] # 失败url
    state = 100 # 爬取状态

    try:
        # ------------爬取检索结果列表 result.json------------
        index_list= [] # 序号
        name_list= [] # 题名
        author_list= [] # 作者
        get_way_list= [] # 来源
        publish_time_list= [] # 发表时间
        db_type_list= [] # 数据库
        cited_list= [] # 被引
        download_list= [] # 下载量
        url_list= [] # url
        crawl_time_list = [] # 爬取时间

        SearchSql = ''  # cnki的查询逻辑 第一页进行查询 后面的页根据SearchSql查询

        # 获取当前线程对象
        current_thread = threading.current_thread()

        # 打印当前线程的名称
        print(f"{current_thread.name}  Keywords:{keywords}")
        for i in range(1, maxpage + 1):

            # 打印当前线程的名称
            #* print(f"{current_thread.name}  开始爬取第{i}页")

            retries = 2  # 每个页面有三次失败重连次数
            while retries >= 0:
                try:
                    content, SearchSql = get_results_text(page=i, keyword=keywords, SearchSql=SearchSql)
                    index_list, name_list, author_list, get_way_list, publish_time_list, db_type_list, cited_list, download_list, url_list, crawl_time_list = parse_results_text(content,i,guid)
                    break
                except Exception as e:
                    if retries > 0:
                        retries -= 1
                        print(f'Error of PAGE: {e}, retrying... ({retries} retries left)')
                        with log_lock:
                            save_log(task_guid=guid, type='异常报错', msg=f'Error of PAGE: {e}, retrying... ({retries} retries left)')
                    else:
                        retries -= 1
                        print(f"ERROR 本页爬取失败: {e}")
                        with log_lock:
                            save_log(task_guid=guid, type='异常报错',msg=f"ERROR 本页爬取失败: {e}")

            save_results(guid, i, index_list, name_list, author_list, get_way_list, publish_time_list, db_type_list,
                         cited_list, download_list, url_list, crawl_time_list)  # 一页一页地存
            success += len(name_list) # 爬取成功数预先+20

            # -------------爬取文献题录信息 Bib.json---------------
            affilation_list = []  # 隶属单位
            page_list = []  # 页码
            pageNum_list = []  # 页数
            abstract_list = []  # 摘要
            keywords_list = []  # 关键词
            publish_time_list = []  # 发表时间

            for url in url_list:
                retries = 2  # 每个url有3次失败重连次数
                while retries >= 0:
                    try:
                        content = get_Bib_text(url)
                        # print("我来到了content下面！")
                        parse_Bib_text(content, affilation_list, page_list, pageNum_list, abstract_list, keywords_list, publish_time_list)
                        break
                    except Exception as e:
                        if retries > 0:
                            retries -= 1
                            traceback.print_exc()  # 打印详细的异常信息
                            print(f'Error of URL : {e}, retrying... ({retries} retries left)')
                            with log_lock:
                                save_log(task_guid=guid, type='异常报错', msg=f'Error of URL : {e}, retrying... ({retries} retries left)')
                        else:
                            retries -= 1
                            fail += 1 # 失败爬取数累加
                            fail_list.append(url)
                            traceback.print_exc()  # 打印详细的异常信息
                            parse_Bib_text_error(affilation_list, page_list, pageNum_list, abstract_list,
                                           keywords_list, publish_time_list)
                            # print("我执行了parse_Bib_text_error!")
                            print(f"Error 该url爬取失败: {e}")
                            with log_lock:
                                save_log(task_guid=guid, type='异常报错', msg=f"Error 该url爬取失败: {e}")
                            print(fail_list)

            save_Bib(guid, index_list, name_list, author_list, affilation_list, get_way_list, page_list, pageNum_list,
                     publish_time_list, db_type_list, cited_list, download_list, abstract_list, keywords_list,
                     url_list, crawl_time_list)
            success -= fail # 预定爬取成功数 - 实际失败爬取数 则为实际爬取成功数
            with spider_and_task_lock:
                save_spider_and_task(guid=guid, keywords=keywords, requester=requester, task_type=task_type, first_at='',
                                     finish_at='', state='200',
                                     success=success, fail=fail, fail_list=fail_list, maxpage=maxpage)    # 更新success fail fail_lsit

            #* print(f"{current_thread.name}  第{i}页爬取完毕！")
            time.sleep(8)


    except Exception as e:
        print(f"Error of crawl：{e}")
        with log_lock:
            save_log(task_guid=guid,type='异常报错', msg=f"Error of crawl：{e}")
        state = 200 # 修改状态码

    finish_at = datetime.datetime.now()  # 记录任务完成时间
    return success, fail, fail_list, state, first_at, finish_at


task_queue = PriorityQueue()

def start(guid,keywords, requester, task_type, maxpage=10):


    while True:
        # 队列出队，开始执行爬虫任务
        task_priority, guid, access_token, keywords, requester, task_type, maxpage = task_queue.get()
        with spider_and_task_lock:
            save_spider_and_task(guid=guid, keywords=keywords, requester=requester, task_type=task_type, first_at='', finish_at='', state='200',
                                 success='0', fail='0', fail_list='', maxpage=maxpage)

        success, fail, fail_list, state, first_at, finish_at = crawl(guid=guid, keywords=keywords,requester=requester, task_type=task_type, maxpage=maxpage)
        with spider_and_task_lock:
            save_spider_and_task(guid=guid, keywords=keywords, requester=requester, task_type=task_type, first_at=first_at,
                                 finish_at=finish_at, state='100',
                                 success=success, fail=fail, fail_list=fail_list, maxpage=maxpage)

        # 更新requester.json
        if state == 100:
            with requester_lock:
                save_requester(requester=requester, successNum=1)
        else:
            with requester_lock:
                save_requester(requester=requester, failNum=1)

        # 存入数据库
        save_to_database(task_guid=guid, maxpage=maxpage)

        # 队列任务完成
        task_queue.task_done()



        print(f"任务：{guid} 完成！")


# 更新requester.json
def save_requester(requester='', last_request='', newTaskNum='', successNum='', failNum='', getStateNum='', getResultNum=''):

        # 先读取现有的 requester.json 文件内容
        try:
            with open('requester.json', 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
        # except (FileNotFoundError, json.decoder.JSONDecodeError):
        except Exception:
            # 如果文件不存在或者不是有效的JSON，则初始化一个空的JSON对象
            json_data = {}

        if requester not in json_data:  # 没有则创建字段
            json_data[str(requester)] = {
                "last_request": str(last_request),
                "newTask": "1",
                "success": "",
                "fail": "",
                "getState": "",
                "getResult": ""
            }
        else:  # 有则更新相关字段
            if last_request != '':
                json_data[str(requester)]['last_request'] = str(last_request)
            if newTaskNum != '':
                newTaskValue = int(json_data[str(requester)]['newTask'].strip() or 0) + 1
                json_data[str(requester)]['newTask'] = str(newTaskValue)
            if successNum != '':
                successValue = int(json_data[str(requester)]['success'].strip() or 0) + 1
                json_data[str(requester)]['success'] = str(successValue)
            if failNum != '':
                failValue = int(json_data[str(requester)]['fail'].strip() or 0) + 1
                json_data[str(requester)]['fail'] = str(failValue)
            if getStateNum != '':
                getStateValue = int(json_data[str(requester)]['getState'].strip() or 0) + 1
                json_data[str(requester)]['getState'] = str(getStateValue)
            if getResultNum != '':
                getResultValue = int(json_data[str(requester)]['getResult'].strip() or 0) + 1
                json_data[str(requester)]['getResult'] = str(getResultValue)
        # 将修改后的数据写回文件
        with open('requester.json', 'w') as json_file:
            json.dump(json_data, json_file, indent=4)

# 更新access_token.json
def save_access_token(task_guid='', name='', requester='', disabled=''):
    # 先读取现有的 access_token.json 文件内容
    try:
        with open('access_token.json', 'r', encoding='utf-8') as json_file:
            json_data = json.load(json_file)
    except Exception:
        # 如果文件不存在或者不是有效的JSON，则初始化一个空的JSON对象
        json_data = {}


    if task_guid not in json_data:  # 没有则创建字段
        json_data[str(task_guid)] = {
            "name": name,
            "guid": requester,
            "disabled": disabled
    }
    #     json_data[str(task_guid)] = {
    #         "name": name,
    #         "guid": requester,
    #         "disabled": disabled,
    #     }
    else:  # 有则更新相关字段
        if disabled != '':
            json_data[str(task_guid)]['disabled'] = str(disabled)
        if name != '':
            json_data[str(task_guid)]['name'] = str(name)
        if task_guid != '':
            json_data[str(task_guid)]['guid'] = str(task_guid)

    # 将修改后的数据写回文件
    with open('access_token.json', 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)


def save_log(task_guid='', type = '', msg = ''):
    # 先读取现有的 log.json 文件内容
    try:
        with open(f'{task_guid}/log.json', 'r', encoding='utf-8') as json_file:
            json_data = json.load(json_file)
    except Exception:
        # 如果文件不存在或者不是有效的JSON，则初始化一个空的JSON对象
        json_data = {}
    json_data[str(datetime.datetime.now())] = {
        'type': str(type),
        'msg': str(msg),
    }

    with open(f'{task_guid}/log.json', 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)

