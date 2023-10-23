# _*_ coding : utf-8 _*_
# @Time : 2023/8/14 18:12
# @Author : djl
# @Project : cnki
# @File : crawler


import requests
from lxml import etree
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import mysql.connector
import time
import datetime
# 写入数据库
def write_data(keyword,source, target_list1, target_list2, target_list3, target_list4, target_list5):
    # 数据库配置
    conn = mysql.connector.connect(
        host='localhost',
        port='3306',
        user='root',
        password='123456',
        database='test'
    )

    cursor = conn.cursor()

    create_table_query = '''
        CREATE TABLE IF NOT EXISTS cnki (
            id INT AUTO_INCREMENT PRIMARY KEY,
            topic VARCHAR(255) COMMENT '文献名',
            author TEXT COMMENT '作者',
            date DATETIME COMMENT '发表时间',
            address VARCHAR(255) COMMENT '源网址',
            summary TEXT COMMENT '摘要',
            keyword VARCHAR(255) COMMENT '关键词',
            source VARCHAR(255) COMMENT '数据来源',
            crawl_time DATETIME COMMENT '爬取时间'
        )
    '''
    cursor.execute(create_table_query)

    insert_query = 'INSERT INTO cnki (topic, author, date, address, summary, keyword, source, crawl_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'

    for i in range(len(target_list1)):
        # 将列表转换为字符串
        summary_str = '\n'.join(target_list5[i])
        data = (target_list1[i], target_list2[i], target_list3[i], target_list4[i], summary_str, keyword, source, datetime.datetime.now())
        # data = (target_list1[i], target_list2[i], target_list3[i], target_list4[i], target_list5[i])
        try:
            cursor.execute(insert_query, data)
            conn.commit()
            print(f"Inserted data for {target_list1[i]}")
        except mysql.connector.Error as err:
            print(f"Error inserting data for {target_list1[i]}: {err}")
            conn.rollback()
        # cursor.execute(insert_query, data)
        # conn.commit()

    cursor.close()
    conn.close()

# 请求:文献名、作者、发表时间、源网址
def request_data(keyword, pageNum, DbCode, resource):
    url = 'https://kns.cnki.net/kns/brief/grid'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
        # 'Cookie': 'Ecp_notFirstLogin=qxuTBW; cangjieStatus_NZKPTKNS72=true; cangjieConfig_NZKPTKNS72=%7B%22status%22%3Atrue%2C%22startTime%22%3A%222023-01-05%22%2C%22endTime%22%3A%222024-01-05%22%2C%22type%22%3A%22mix%22%2C%22poolSize%22%3A10%2C%22intervalTime%22%3A10000%2C%22persist%22%3Afalse%7D; Ecp_ClientId=c230613151300884073; knsLeftGroupSelectItem=1%3B2%3B; Ecp_ClientIp=112.36.83.191; SID_sug=128006; ASP.NET_SessionId=uflmf43jd4uo3dpautvqckal; CurrSortFieldType=desc; eng_k55_id=015106; pageReferrInSession=https%3A//www.cnki.net/; firstEnterUrlInSession=https%3A//kns.cnki.net/kns8/defaultresult/index; VisitorCapacity=1; CurrSortField=%e7%9b%b8%e5%85%b3%e5%ba%a6%2frelevant%2c(%e5%8f%91%e8%a1%a8%e6%97%b6%e9%97%b4%2c%27time%27)+desc; Ecp_showrealname=1; Ecp_loginuserbk=dx1119; Ecp_notFirstLogin=uveNAU; personsets=; SID_kns8=123154; LID=WEEvREcwSlJHSldSdmVqMDh6c3VIMHlkdjg4bk5lc2hnLzg2ZXp3T3BBQT0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4ggI8Fm4gTkoUKaID8j8gFw!!; Ecp_LoginStuts={"IsAutoLogin":false,"UserName":"dx1119","ShowName":"%E6%B5%8E%E5%8D%97%E5%A4%A7%E5%AD%A6","UserType":"bk","BShowName":"","r":"qxuTBW","Members":[]}; Ecp_session=1; dblang=ch; SID_kns_new=kns15128006; c_m_LinID=LinID=WEEvREcwSlJHSldSdmVqMDh6c3VIMHlkdjg4bk5lc2hnLzg2ZXp3T3BBQT0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4ggI8Fm4gTkoUKaID8j8gFw!!&ot=09%2F14%2F2023%2019%3A39%3A33; c_m_expire=2023-09-14%2019%3A39%3A33'
    }
    data = {
        'QueryJson': '{"Platform":"","Resource":"'+resource+'","DBCode":"'+DbCode+'","KuaKuCode":"CJZK,CDFD,CMFD,CPFD,IPFD,CCND,BDZK,CPVD","QNode":{"QGroup":[{"Key":"Subject","Title":"","Logic":0,"Items":[{"Field":"SU","Value":"'+keyword+'","Operator":"TOPRANK","Logic":0}],"ChildItems":[]}]},"ExScope":1,"SearchType":"0"}',
        'DbCode': DbCode,
        'pageNum': pageNum,
        'pageSize': '20',
        'sortType': 'desc',
        'boolSearch': 'true',
        'version': 'kns7',
        'productStr': 'CJZK,CDFD,CMFD,CPFD,IPFD,CCND,BDZK,CPVD',
        'sentenceSearch': 'false',
        'aside': '主题:'+keyword
    }

    response = requests.post(url=url, data=data, headers=headers)
    response.encoding = 'utf-8'
    content = response.text
    return content

# 解析：文献名、作者、发表时间、源网址
def parsing_data(content):
    tree = etree.HTML(content)
    soup = BeautifulSoup(content, 'lxml')
    # 获取所有文献名
    topic_list_labels = tree.xpath('//a[@class="fz14"]')
    topic_list = []
    for topic_element in topic_list_labels:
        topic_item = topic_element.xpath('normalize-space(.)')
        topic_list.append(topic_item)

    # 获取所有作者名
    author_list_labels = tree.xpath('//tr//td[@class="author"]')
    author_list = []
    for author_element in author_list_labels:
        author_item = author_element.xpath('normalize-space(.)')
        author_list.append(author_item)


    # 获取发表时间
    # date_list_labes = tree.xpath('//tr//td[@class="date"]')
    # date_list_labes = tree.xpath('//td[@class="date"]')
    # print(date_list_labes)
    # date_list = []
    # for time_element in date_list_labes:
    #     time_item = time_element.xpath('normalize-space(.)')
    #     date_list.append(time_item)
    # date_list = soup.select('.date')
    date_list = tree.xpath('//td[@class="date"]/text()')

    # 获取源网址
    address_list = tree.xpath('//a[@class="fz14"]/@href')

    # print(topic_list)
    # print(author_list)
    # print(date_list)
    # print(address_list)
    # print(len(topic_list), len(author_list), len(date_list), len(address_list), len(summary_list))
    return topic_list, author_list, date_list, address_list

# 请求:摘要
def request_data2(url):
    url = url
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
        'Cookie': 'language=gb; cangjieStatus_NZKPT2=true; cangjieConfig_NZKPT2=%7B%22status%22%3Atrue%2C%22startTime%22%3A%222022-10-20%22%2C%22endTime%22%3A%222023-10-20%22%2C%22orginHosts%22%3A%22kns.cnki.net%22%2C%22type%22%3A%22mix%22%2C%22poolSize%22%3A%2210%22%2C%22intervalTime%22%3A10000%2C%22persist%22%3Afalse%7D; Ecp_notFirstLogin=qxuTBW; Ecp_ClientId=c230613151300884073; knsLeftGroupSelectItem=1%3B2%3B; Ecp_ClientIp=112.36.83.191; SID_sug=128006; ASP.NET_SessionId=uflmf43jd4uo3dpautvqckal; CurrSortFieldType=desc; eng_k55_id=015106; pageReferrInSession=https%3A//www.cnki.net/; firstEnterUrlInSession=https%3A//kns.cnki.net/kns8/defaultresult/index; VisitorCapacity=1; CurrSortField=%e7%9b%b8%e5%85%b3%e5%ba%a6%2frelevant%2c(%e5%8f%91%e8%a1%a8%e6%97%b6%e9%97%b4%2c%27time%27)+desc; Ecp_showrealname=1; Ecp_loginuserbk=dx1119; Ecp_notFirstLogin=uveNAU; personsets=; SID_kns8=123154; LID=WEEvREcwSlJHSldSdmVqMDh6c3VIMHlkdjg4bk5lc2hnLzg2ZXp3T3BBQT0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4ggI8Fm4gTkoUKaID8j8gFw!!; Ecp_LoginStuts={"IsAutoLogin":false,"UserName":"dx1119","ShowName":"%E6%B5%8E%E5%8D%97%E5%A4%A7%E5%AD%A6","UserType":"bk","BShowName":"","r":"qxuTBW","Members":[]}; Ecp_session=1; dblang=ch; SID_kns_new=kns25128005; c_m_LinID=LinID=WEEvREcwSlJHSldSdmVqMDh6c3VIMHlkdjg4bk5lc2hnLzg2ZXp3T3BBQT0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4ggI8Fm4gTkoUKaID8j8gFw!!&ot=09%2F14%2F2023%2019%3A39%3A13; c_m_expire=2023-09-14%2019%3A39%3A13'
    }
    response = requests.get(url=url, headers=headers)
    content = response.text
    return content

# 解析:摘要
def parsing_data2(content):
    tree = etree.HTML(content)
    # 获取摘要
    summary = tree.xpath('//span[@id="ChDivSummary"]/text()')
    # 获取PDF下载链接
    pdf_link = tree.xpath('//a[@id="pdfDown"]/@href')

    return summary,pdf_link


# 下载 pdf (目前未实现)
def download_pdf(pdf_link, pdf_filename):
    print(pdf_link)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
        'Cookie': 'language=gb; cangjieStatus_NZKPT2=true; cangjieConfig_NZKPT2=%7B%22status%22%3Atrue%2C%22startTime%22%3A%222022-10-20%22%2C%22endTime%22%3A%222023-10-20%22%2C%22orginHosts%22%3A%22kns.cnki.net%22%2C%22type%22%3A%22mix%22%2C%22poolSize%22%3A%2210%22%2C%22intervalTime%22%3A10000%2C%22persist%22%3Afalse%7D; Ecp_notFirstLogin=qxuTBW; Ecp_ClientId=c230613151300884073; knsLeftGroupSelectItem=1%3B2%3B; Ecp_ClientIp=112.36.83.191; SID_sug=128006; ASP.NET_SessionId=uflmf43jd4uo3dpautvqckal; CurrSortFieldType=desc; eng_k55_id=015106; pageReferrInSession=https%3A//www.cnki.net/; firstEnterUrlInSession=https%3A//kns.cnki.net/kns8/defaultresult/index; VisitorCapacity=1; CurrSortField=%e7%9b%b8%e5%85%b3%e5%ba%a6%2frelevant%2c(%e5%8f%91%e8%a1%a8%e6%97%b6%e9%97%b4%2c%27time%27)+desc; Ecp_showrealname=1; Ecp_loginuserbk=dx1119; Ecp_notFirstLogin=uveNAU; personsets=; SID_kns8=123154; LID=WEEvREcwSlJHSldSdmVqMDh6c3VIMHlkdjg4bk5lc2hnLzg2ZXp3T3BBQT0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4ggI8Fm4gTkoUKaID8j8gFw!!; Ecp_LoginStuts={"IsAutoLogin":false,"UserName":"dx1119","ShowName":"%E6%B5%8E%E5%8D%97%E5%A4%A7%E5%AD%A6","UserType":"bk","BShowName":"","r":"qxuTBW","Members":[]}; Ecp_session=1; dblang=ch; SID_kns_new=kns15128006; c_m_LinID=LinID=WEEvREcwSlJHSldSdmVqMDh6c3VIMHlkdjg4bk5lc2hnLzg2ZXp3T3BBQT0=$9A4hF_YAuvQ5obgVAqNKPCYcEjKensW4ggI8Fm4gTkoUKaID8j8gFw!!&ot=09%2F14%2F2023%2019%3A39%3A43; c_m_expire=2023-09-14%2019%3A39%3A43'
    }
    pdf_response = requests.get(url=pdf_link,headers=headers)
    if pdf_response.status_code == 200:
        with open(pdf_filename, "wb") as pdf_file:
            pdf_file.write(pdf_response.content)
        print(f"Downloaded PDF: {pdf_filename}")
    else:
        print(f"Failed to download PDF from link: {pdf_link}")

# 爬取
def crawl(selected_db, keyword):
    url = 'https://kns.cnki.net/kns/search?dbcode={}'.format(selected_db['code'])
    # 创建浏览器对象
    path = 'chromedriver.exe'
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('‐‐disable‐gpu')
    browser = webdriver.Chrome(options=chrome_options)
    # browser = webdriver.Chrome(path)
    browser.get(url)
    time.sleep(2)

    # 获取文本框对象
    input = browser.find_element_by_id('txt_1_value1')
    input.send_keys(keyword)
    input_button = browser.find_element_by_id('btnSearch')
    input_button.click()
    time.sleep(5)

    # 获取所有结果数
    results_number = int(
        browser.find_element_by_xpath("//div[@id='countPageDiv']/span[@class='pagerTitleCell']/em").text)

    # 初始化
    # 获取当前窗口的句柄（原始窗口）
    original_window_handle = browser.current_window_handle
    # 初始化当前页码
    pageNum = 1
    while True:
        time.sleep(4)
        if (pageNum > 10):
            break
        # 发送请求
        content = request_data(keyword, pageNum, selected_db['code'], selected_db['resource'])
        # 解析数据
        topic_list, author_list, date_list, address_list = parsing_data(content)
        # 获取摘要和pdf
        # 摘要results
        summary_list = []
        # 处理最后一页的情况
        right = 20
        if (pageNum == results_number - results_number % 20):
            right = results_number % 20
        for i in range(1, right + 1):
            time.sleep(4)
            second_link_element = browser.find_element_by_xpath(
                '//tr[' + str(i) + ']/td[@class="name"]/a[@class="fz14"]')
            second_link = second_link_element.get_attribute('href')
            second_link_element.click()
            time.sleep(2)

            # 获取所有窗口句柄
            window_handles = browser.window_handles

            if len(window_handles) > 1:
                # 切换到新窗口
                new_window_handle = window_handles[-1]
                browser.switch_to.window(new_window_handle)
                time.sleep(3)
                # 请求摘要
                content2 = request_data2(second_link)
                # 解析摘要，组建summary_list
                summary_item, pdf_link = parsing_data2(content2)
                summary_list.append(summary_item)
                # 下载pdf
                # download_pdf(pdf_link, topic_list[i])
                # 关闭当前窗口
                browser.close()
                # 切换回原始窗口
                browser.switch_to.window(original_window_handle)
        # 写入数据库
        # print(summary_list)
        write_data(keyword,selected_db['name'],topic_list, author_list, date_list, address_list, summary_list)
        print('第{}次写入数据库成功！'.format(pageNum))
        # 滑到底部
        time.sleep(3)
        js_bottom = 'document.documentElement.scrollTop=100000'
        browser.execute_script(js_bottom)
        time.sleep(3)

        # 获取下一页的按钮
        next = browser.find_element_by_id('PageNext')
        # 点击下一页
        pageNum += 1
        next.click()


if __name__ == '__main__':
    # 数据库列表
    db = [
        {'id':1,'name': '文献', 'code': 'SCDB', 'resource':'CROSSDB', 'ok':1},
        {'id':2,'name': '学术期刊', 'code': 'CJZK','resource':'JOURNAL', 'ok':1},
        {'id':3,'name': '博硕士学位论文', 'code': 'CDMD','resource':'DISSERTATION', 'ok':0},
        {'id':4,'name': '博士学位论文', 'code': 'CDFD','resource':'DISSERTATION', 'ok':0},
        {'id':5,'name': '硕士学位论文', 'code': 'CMFD','resource':'DISSERTATION', 'ok':0},
        {'id':6,'name': '会议论文', 'code': 'CIPD','resource':'CONFERENCE', 'ok':1},
        {'id':7,'name': '中国重要报纸全文数据库', 'code': 'CCND','resource':'NEWSPAPER', 'ok':1},
        {'id':8,'name': '年鉴', 'code': 'CYFD','resource':'ALMANAC', 'ok':0},
        {'id':9,'name': '国内会议论文', 'code': 'CPFD','resource':'CONFERENCE', 'ok':1},
        {'id':10,'name': '国际会议论文全文数据库', 'code': 'CIPD','resource':'CONFERENCE', 'ok':1},
        {'id':11,'name': '国际会议论文', 'code': 'IPFD','resource':'CONFERENCE', 'ok':1},
        {'id':12,'name': '会议视频', 'code': 'CPVD','resource':'CONFERENCE', 'ok':1},
        {'id':13,'name': '专利数据总库', 'code': 'SCOD','resource':'PATENT', 'ok':0},
        {'id':14,'name': '中国专利数据库', 'code': 'SCPD','resource':'PATENT', 'ok':0},
        {'id':15,'name': '海外专利摘要数据库（知网版）', 'code': 'SOPD','resource':'PATENT', 'ok':0},
        {'id':16,'name': '标准数据总库', 'code': 'CISD','resource':'STANDARD', 'ok':0},
        {'id':17,'name': '国家标准全文数据库', 'code': 'SCSF','resource':'STANDARD', 'ok':0},
        {'id':18,'name': '中国行业标准全文数据库', 'code': 'SCHF','resource':'STANDARD', 'ok':0},
        {'id':19,'name': '国内外标准数据库', 'code': 'SMSD','resource':'STANDARD', 'ok':0},
        {'id':20,'name': '中国科技项目创新成果鉴定意见数据库（知网版）', 'code': 'SNAD','resource':'ACHIEVEMENTS', 'ok':0},
        {'id':21,'name': '图片', 'code': 'IMAGE','resource':'', 'ok':0},
        {'id':22,'name': '国学宝典段落数据库', 'code': 'GXDB_SECTION','resource':'', 'ok':0},
        {'id':23,'name': '国学宝典数据库', 'code': 'GXDB','resource':'', 'ok':0},
        {'id':24,'name': '中国学术辑刊全文数据库', 'code': 'CCJD','resource':'', 'ok':0},
        {'id':25,'name': 'Frontiers系列期刊数据库', 'code': 'FCJD','resource':'', 'ok':0},
        {'id':26,'name': '国学宝典段落数据库', 'code': 'GXDB_SECTION','resource':'', 'ok':0},
        {'id':27,'name': '图书', 'code': 'BDZK','resource':'BOOK', 'ok':1},
        {'id':28,'name': '中文图书', 'code': 'WBFD','resource':'BOOK', 'ok':1},
        {'id':29,'name': '中文图书章节', 'code': 'WBFD_SECTION','resource':'BOOK', 'ok':1},
        {'id':30,'name': '外文图书', 'code': 'WWBD','resource':'BOOK', 'ok':1}
    ]
    # 展示数据库选项给用户
    print("可用数据库选项：")
    for entry in db:
        if entry['ok'] == 1:
            print(f"{entry['id']}. {entry['name']}")
    # 获取主体词及所需查询数据库
    keyword = input("请输入主题词：")
    database_id = int(input("请输入需要查询的数据库编号："))
    selected_db = None

    for entry in db:
        if entry['id'] == database_id:
            selected_db = entry
            break

    if selected_db:
        print(f"您选择的数据库是：{selected_db['name']}，代码为：{selected_db['code']}")
        print("正在爬取...")
        # 进行爬取
        crawl(selected_db, keyword)
    else:
        print("无效的数据库编号。")


