from flask import Flask, request, jsonify
from psj import *

from concurrent.futures import ThreadPoolExecutor
from flask_cors import CORS
from flask_cors import cross_origin
app = Flask(__name__)
CORS(app)
CORS(app, resources={r"/*": {"origins": "http://localhost:8000/*"}})


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


# 创建一个线程池，上限为5个线程
thread_pool = ThreadPoolExecutor(max_workers=5)



@app.route('/cnkiSpider/newtask', methods=['POST'])
@cross_origin()
def newtask():


    global task_queue

    if request.method == 'POST':
        # 获取POST请求中的数据
        access_token = request.form.get('access_token')
        keywords = request.form.get('keywords')
        requester = request.form.get('requester')
        task_type = request.form.get('task_type')
        maxpage = int(request.form.get('maxpage'))
        if maxpage == 0: maxpage = 5

    guid = ''
    # 返回
    response = {
        'access_token': str(access_token),
        'keywords': str(keywords),
        'requester': str(requester),
        'task_type': str(task_type),
        'maxpage': str(maxpage),
        'code': '100',  # 状态码
        'task_guid': str(guid),  # 任务guid
        'msg': '操作成功',  # 返回信息
        'status': ''  # 状态
    }



    # 验证200：系统内部错误
    try:
        # 验证密钥
        # if access_token != '123':  # 检查access_token是否不等于123
        #     raise ValueError('Invalid access_token')  # 引发自定义异常
        ################################################
        with open('access_token.json', 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        # 密钥表里没有该密钥，临时设置（后期可以预先和用户设定好密钥表）
        if access_token not in json_data:
            # item = {
            #     "name": "内毒素",
            #     "guid": str(requester),
            #     "disabled": "False"
            # }
            # json_data[str(access_token)] = item
            # with open('access_token.json', 'w', encoding='utf-8') as f:
            #     json.dump(json_data, f, ensure_ascii=False, indent=4)
            raise ValueError('No such access_token')  #（后期可以预先和用户设定好密钥表，这样只需报这句异常）
        # 密钥表有该密钥，
        else:
            requester_guid = json_data[str(access_token)]['guid']
            disabled = json_data[str(access_token)]['disabled']
            if requester_guid != requester:     # 检查该用户是否享有该密钥
                raise ValueError('You do not own this access_token')  # 引发自定义异常
            if disabled == 'True':  # 检查该密钥是否禁用
                raise ValueError('The access_token is disabled')  # 引发自定义异常

        ################################################
        # 处理新任务，入队
        guid = uuid.uuid4()  # 为新任务分配guid
        response['task_guid'] = str(guid)   # 更新task_guid

        if not os.path.exists(str(guid)):
            os.makedirs(str(guid))

        with requester_lock:
            # 更新 requester.json 的last_request字段
            save_requester(requester=requester, last_request=datetime.datetime.now(), newTaskNum=1)
        # with access_token_lock:
        #     # 更新 access_token.json
        #     save_access_token(task_guid=guid, name='内毒素', requester=requester, disabled='False')

        if task_type == 'realtime':
            task_queue_item = (0, guid, access_token, keywords, requester, task_type, maxpage)  # 紧急队列
        else:
            task_queue_item = (1, guid, access_token, keywords, requester, task_type, maxpage)  # 非紧急队列

        # 使用锁来确保多线程安全，这意味着在有其他线程正在访问任务队列时，当前线程将会等待锁释放后再执行任务入队操作
        with task_queue_lock:
            task_queue.put(task_queue_item)


        # 修改status
        queue_size = thread_pool._work_queue.qsize()    # 等待执行的任务数量

        if queue_size == 0 and task_type == 'realtime':
            response['status'] = '0'    # 添加任务成功且立即执行：0
        elif queue_size > 0 and task_type == 'realtime':
            response['status'] = '1'    # 添加成功已有爬虫任务未启动：1
            save_spider_and_task(guid=guid, keywords=keywords, requester=requester, task_type=task_type, first_at='',
                                 finish_at='', state='300',
                                 success='0', fail='0', fail_list='', maxpage=maxpage)
        elif queue_size == 0 and task_type == 'batch':
            response['status'] = '2'    # 批处理任务添加成功且立即执行：2
        elif queue_size > 0 and task_type == 'batch':
            response['status'] = '3'  # 批处理任务添加成功且已有爬虫任务未启动：3
            save_spider_and_task(guid=guid, keywords=keywords, requester=requester, task_type=task_type, first_at='',
                                 finish_at='', state='300',
                                 success='0', fail='0', fail_list='', maxpage=maxpage)
    except ValueError as ve:
        # 捕获密钥异常
        print(f'Error: Invalid access_token: {ve}')
        response['code'] = '200'  # 修改状态码
        response['msg'] = f'操作失败，失败原因：{ve}'
        # 密钥不对，不写入任何json信息
        del response['access_token']
        del response['keywords']
        del response['requester']
        del response['task_type']
        del response['maxpage']
        return response

    except Exception as e:
        print(f'Error of newTask:  {e}')
        traceback.print_exc()
        response['code'] = '200'  # 修改状态码
        response['msg'] = f'操作失败，失败原因：{e}'


    # 使用线程池提交任务，线程池会自动管理并发线程数量
    thread_pool.submit(start, guid, keywords, requester, task_type, maxpage)


    with log_lock:  # 更新 log.json
        save_log(task_guid=guid, type='任务启动', msg=response)

    # 删掉请求头
    del response['access_token']
    del response['keywords']
    del response['requester']
    del response['task_type']
    del response['maxpage']

    return response


@app.route('/cnkiSpider/getState', methods=['post'])
@cross_origin()
def getState():

    if request.method == 'POST':
        # 获取POST请求中的数据
        access_token = request.form.get('access_token')
        task_guid = request.form.get('task_guid')

    # 返回值
    response = {
        'access_token':access_token,
        'task_guid':task_guid,
        'code': '100',  # 状态码
        'msg': '操作成功',  # 返回信息
        'data': ''  # 爬虫任务状态
    }

    # 验证200：系统内部错误
    try:

        # # 验证密钥
        # if access_token != '123':  # 检查access_token是否不等于123
        #     raise ValueError('Invalid access_token')  # 引发自定义异常



        # 指定要打开的 JSON 文件的路径
        json_file_path = f'{task_guid}/spider.json'

        # 验证300：task_guid不存在
        try:
            # 打开 task.json 文件
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)  # 解析 JSON 文件为 Python 对象

                keywords = json_data['keywords']
                requester = json_data['requester']
                task_type = json_data['task_type']
                first_at = json_data['first_at']
                finish_at = json_data['finish_at']
                state = json_data['state']
                success = json_data['success']
                fail = json_data['fail']
                fail_list = json_data['fail_list']
                maxpage = json_data['maxpage']

        except Exception as e:
            print(f"Error of getState:{e}")
            response['code'] = '300'
            response['msg'] = '任务guid不存在'
            del response['access_token']
            del response['task_guid']
            return response



        ################################################
        with open('access_token.json', 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        # 密钥表里没有该密钥，临时设置（后期可以预先和用户设定好密钥表）
        if access_token not in json_data:
            raise ValueError('No such access_token')  # （后期可以预先和用户设定好密钥表，这样只需报这句异常）
        # 密钥表有该密钥，
        else:
            requester_guid = json_data[str(access_token)]['guid']
            disabled = json_data[str(access_token)]['disabled']
            if requester_guid != requester:  # 检查该用户是否享有该密钥
                raise ValueError('You do not own this access_token')  # 引发自定义异常
            if disabled == 'True':  # 检查该密钥是否禁用
                raise ValueError('The access_token is disabled')  # 引发自定义异常

        ################################################


        item = {
            "taskGuid": task_guid,
            "keywords": keywords,
            "requester": requester,
            "taskType": task_type,
            "firstAt": first_at,
            "finishAt": finish_at,
            "state": state,
            "success": success,
            "fail": fail,
            "failList": fail_list,
            "maxpage": maxpage,
        }
        response['data'] = item

    except ValueError as ve:
        # 捕获自定义异常
        print(f'Error: Invalid access_token: {ve}')
        response['code'] = '200'  # 修改状态码
        response['msg'] = f'操作失败，失败原因：{ve}'
        del response['access_token']    # 密钥不对，直接返回，不写入任何json信息
        del response['task_guid']
        return response

    except Exception as e:
        print(f"Error of getState:{e}")
        response['code'] = '200'
        response['msg'] = f'操作失败，失败原因：{e}'

    with requester_lock:    # 更新requester.json
        save_requester(requester=requester, getStateNum=1)

    del response['access_token']
    del response['task_guid']
    with log_lock:   # 更新log.json
        save_log(task_guid=task_guid, type='状态查询', msg=response)

    return response

@app.route('/cnkiSpider/getResult', methods=['post'])
@cross_origin()
def getResult():

    if request.method == 'POST':
        # 获取POST请求中的数据
        access_token = request.form.get('access_token')
        task_guid = request.form.get('task_guid')

    # 返回值
    response = {
        'access_token':access_token,
        'task_guid':task_guid,
        'code': '100',  # 状态码
        'msg': '操作成功',  # 返回信息
        'data': ''  # 爬取数据结果JSON数组/列表，每个元素是一个文献结果。
    }

    # 验证code 200 内部错误
    try:

        # if access_token != '123':  # 检查access_token是否不等于123
        #     raise ValueError('Invalid access_token')  # 引发自定义异常

        # 验证code 300 guid不存在
        try:
            with open('task.json', 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
                maxpage = json_data[str(task_guid)]['maxpage']
                requester = json_data[str(task_guid)]['requester']
        except Exception as e:
            response['code'] = '300'
            response['msg'] = '任务guid不存在'
            del response['access_token']
            del response['task_guid']
            return response

        ################################################
        with open('access_token.json', 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        # 密钥表里没有该密钥，临时设置（后期可以预先和用户设定好密钥表）
        if access_token not in json_data:
            raise ValueError('No such access_token')  # （后期可以预先和用户设定好密钥表，这样只需报这句异常）
        # 密钥表有该密钥，
        else:
            requester_guid = json_data[str(access_token)]['guid']
            disabled = json_data[str(access_token)]['disabled']
            if requester_guid != requester:  # 检查该用户是否享有该密钥
                raise ValueError('You do not own this access_token')  # 引发自定义异常
            if disabled == 'True':  # 检查该密钥是否禁用
                raise ValueError('The access_token is disabled')  # 引发自定义异常
        ################################################

        # 验证code 400 任务未完成，Bib.json文件不存在
        json_file_path = f'{task_guid}/Bib.json'
        try:

            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                json_data = json.load(json_file)
            bib = {}

            for i in range(1, int(maxpage) * 20 + 1):
                item = {
                    'name': json_data[f'{i}']['name'],
                    'author': json_data[f'{i}']['author'],
                    'affiliation': json_data[f'{i}']['affiliation'],
                    'get_way': json_data[f'{i}']['get_way'],
                    'page': json_data[f'{i}']['page'],
                    'pageNum': json_data[f'{i}']['pageNum'],
                    'publish_time': json_data[f'{i}']['publish_time'],
                    'db_type': json_data[f'{i}']['db_type'],
                    'cited': json_data[f'{i}']['cited'],
                    'download': json_data[f'{i}']['download'],
                    'abstract': json_data[f'{i}']['abstract'],
                    'keywords': json_data[f'{i}']['keywords'],
                    'url': json_data[f'{i}']['url'],
                    'crawl_time': json_data[f'{i}']['crawl_time']
                }
                bib[str(i)] = item
            response['data'] = bib


        except Exception as e:
            print(f"Error of getResult:{e}")
            response['code'] = '400'
            response['msg'] = '任务未完成'
            with requester_lock:
                # 更新requester.json
                save_requester(requester=requester, getResultNum=1)
            # 更新log.json
            del response['access_token']
            del response['task_guid']
            with log_lock:
                save_log(task_guid=task_guid, type='结果获取', msg=response)
            return response

    except ValueError as ve:
        # 捕获自定义异常
        print(f'Error: Invalid access_token: {ve}')
        response['code'] = '200'  # 修改状态码
        response['msg'] = f'操作失败，失败原因：{ve}'
        del response['access_token']
        del response['task_guid']
        return response

    except Exception as e:
        print(f"Error of getResult:{e}")
        response['code'] = '200'
        response['msg'] = f'操作失败，失败原因：{e}'


    with requester_lock:     # 更新requester.json
        save_requester(requester=requester, getResultNum=1)

    del response['access_token']
    del response['task_guid']
    with log_lock:   # 更新log.json
        save_log(task_guid=task_guid, type='结果获取', msg=response)

    return response


# 创建文件 requester.json
def makedir():

    try:
        if not os.path.isfile('requester.json'):
            # 如果文件不存在，创建一个空的JSON文件
            with open('requester.json', "w") as json_file:
                json.dump({}, json_file)  # 创建一个空的JSON对象

        if not os.path.isfile('access_token.json'):
            with open('access_token.json','w') as json_file:
                json.dump({}, json_file)

        if not os.path.isfile('task.json'):
            with open('task.json','w') as json_file:
                json.dump({}, json_file)
    except Exception as e:
        print(str(e))

if __name__ == '__main__':

    makedir()
    app.run(host='0.0.0.0', port=8000)


