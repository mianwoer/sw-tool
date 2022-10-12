import json
import logging
import multiprocessing
import sys
import time
from time import sleep

import requests
from kafka import KafkaProducer, KafkaConsumer
import configparser
import os
from multiprocessing import Process, cpu_count
import pandas as pd


# 提供消息体，用于传入提取声纹的kafka topic
def prepare_message(item) -> (str, str):
    fileName = item.split('/')[-1]
    aid = fileName.split('.')[0]
    ppwd = item.split('/')[-2].split('.')[0]
    msg = {
        "body": {
            "audios": [
                {
                    "aid": aid,
                    "bits": 16,
                    "chnl": 1,
                    "encoding": 1,
                    "offset": 0,
                    "rate": 8000,
                    "spkn": 1,
                    "uri": item
                }
            ],
            "fetchAudioData": 'false',
            "id": aid,
            "option": 128,
            "tags": {}
        },
        "properties": {
            "fileInfo": {
                "app": "sw-tool",
                "uri": item,
                "ppwd": ppwd,
                "fileName": fileName
            }
        }
    }
    return aid, json.dumps(msg).encode('utf-8')


# 向输入队列发送消息
def send_to_kfk(text, input_topic, kafka_server):
    logger = get_logger()
    producer = KafkaProducer(bootstrap_servers=kafka_server)
    aid, message = prepare_message(text)
    producer.send(input_topic, key=bytes(aid, 'utf-8'), value=message)
    logger.info('正在向kfk队列发送文件 %s 的请求消息体' % aid)


# 向输出队列获取消息(持续获取)，消费队列
def recv_from_kfk(topic: str, bootstrap_servers: str, auto_offset_reset='latest', timeout_ms=10000, file_num=None,
                  file=None):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset=auto_offset_reset,
                             consumer_timeout_ms=timeout_ms)
    # consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset=auto_offset_reset)
    count = 0
    logger = get_logger()
    for message in consumer:
        key = str(message.key, 'utf-8')
        value = json.loads(str(message.value, 'utf-8'))
        fileinfo = value.get('properties').get('fileInfo')
        fileinfo = json.loads(fileinfo)
        body = json.loads(value.get('body'))
        # is_success = body.get('body')[0].get('tracks')[0].get('state').get('success')  # True:成功，False:失败
        # is_success = body.get('body')[0].get('tracks')[0].get('items')[0].get('model').get('state').get('success') # True:成功，False:失败
        app = fileinfo.get('app')
        fileName = fileinfo.get('fileName')
        if app == 'sw-tool' and file_num:
            try:
                sw = body.get('body')[0].get('tracks')[0].get('items')[0].get('model').get('model')
                sw = 'task_failed' if sw is None else sw
            except Exception as e:
                # raise '文件：%s 的声纹提取失败！标记task_failed' % fileName
                sw = 'task_failed'

            if count == 0:
                with open(file, 'w', encoding='utf8') as f:
                    f.write(fileName + ' ' + sw)
                    logger.info('正在保存文件：%s的声纹' % fileName)
                    count += 1
            elif count <= file_num - 1:
                with open(file, 'a', encoding='utf8') as f:
                    f.write('\n' + fileName + ' ' + sw)
                    logger.info('正在保存文件：%s的声纹' % fileName)
                    count += 1

        if count == file_num:  # 数据量与输入的一致则直接退出消费，否则通过 consumer_timeout_ms 等待超时后退出
            logger.info('该批次数据声纹提取结束')
            break
    # KafkaConsumer.close()


def num_of_file(file_url_list) -> int:
    """
        入参：文件路径，
        输出：文本内容行数，即文件数量
    """
    count = 0
    with open(file_url_list, 'r') as f:
        for _ in f.read().splitlines():
            count += 1
    return count


def get_logger():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sw_logger = logging.getLogger('SW-TOOL')
    return sw_logger


def process_compare(sample, sample_sw, trash_lib, trash_lib_sw, request_url, headers):
    # def process_compare(_list):
    body = "[" + '\"' + sample_sw + '\",\"' + trash_lib_sw + '\"' + "]"
    response = requests.post(url=request_url, headers=headers, data=body)
    # response = requests.post(url=_list[4], headers=_list[5], data=body)
    try:
        # score.append(float(ret))  # 记录得分
        return sample, json.loads(response.text).get('body')
    except Exception as e:
        logger.info(sample + " 与 " + trash_lib + "声纹比对失败，请核查！")
        raise e


def collect_data(_tuple: tuple):
    global scores
    scores.append(_tuple[1])


def error_callback_func(err):
    print('出错啦！！%s' % str(err))


if __name__ == '__main__':
    multiprocessing.freeze_support()
    """
    使用python直接运行脚本，可以使用  __file__
    如果打包成exe在windows运行，需要使用  os.path.realpath(sys.argv[0])
    """
    # project_path = os.path.dirname(__file__)
    project_path = os.path.dirname(os.path.realpath(sys.argv[0]))
    start_time = time.time()  # 记录开始时间
    """生成日志对象"""
    logger = get_logger()

    if True:
        """读取配置文件"""
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.join(project_path, 'config'), 'config.ini'), encoding='utf-8')
        consumer_timeout_ms = int(config['config']['consumer_timeout_ms'])
        kafka_server = config['config']['kafka_server']
        input_topic = config['config']['input_topic']
        output_topic = config['config']['output_topic']
        import_file = os.path.join(project_path, config['config']['import'])
        output_file = os.path.join(project_path, config['config']['export'])
        # 读取垃圾音库的配置项，
        trash_libs_sw = os.path.join(project_path, config['config']['trash_libs_sw'])
        trash_libs_urls = os.path.join(project_path, config['config']['trash_libs_urls'])
        # 根据output_file，trash_libs_sw输出的声纹比对结果
        sw_compare_result = os.path.join(project_path, config['config']['sw_compare_result'])
        sw_compare_api = config['config']['sw_compare']
        systemWeight = config['config']['systemWeight']
        try:
            proc_num = int(config['config']['proc_num'])
        except Exception:
            proc_num = cpu_count()

    """如果垃圾音声纹库不存在，则调用kfk（引擎）获取"""
    if not os.path.exists(trash_libs_sw):
        """
        如果垃圾音库的sw数据，即trash_libs_sw配置项的文本不存在，则执行以下任务
        """
        logger.info('开始注册垃圾音声纹库。。。 本次需要注册声纹 %d 条' % num_of_file(trash_libs_urls))
        p1 = Process(target=recv_from_kfk,
                     args=(output_topic, kafka_server, 'latest', consumer_timeout_ms, num_of_file(trash_libs_urls),
                           trash_libs_sw))
        p1.start()
        logger.info("开始注册垃圾音声纹库")
        sleep(3)  # 睡眠一段时间，给消费程序充足的连接准备时间，避免马上生产消息导致最早的部分数据丢失
        # 回到主进程生产消息到kfk输入队列,计算垃圾音库的sw
        with open(trash_libs_urls, 'r') as f:
            for item in f.read().splitlines():
                send_to_kfk(item, input_topic, kafka_server)
        p1.join()
        logger.info("注册垃圾音声纹库完毕")

    """如果样本文件声纹不存在，则开始获取样本的声纹数据"""
    if not os.path.exists(output_file):
        p2 = Process(target=recv_from_kfk,
                     args=(
                         output_topic, kafka_server, 'latest', consumer_timeout_ms, num_of_file(import_file),
                         output_file))
        p2.start()
        logger.info('开始生成样本的声纹数据')
        sleep(3)  # 睡眠一段时间，给消费程序充足的连接准备时间，避免马上生产消息导致最早的部分数据丢失
        with open(import_file, 'r') as f:
            for item in f.read().splitlines():
                send_to_kfk(item, input_topic, kafka_server)
        p2.join()
        logger.info('样本数据的声纹已提取，请查看输出文件')

    """开始进行声纹比对"""
    logger.info("开始进行声纹比对,自动过滤没有声纹的数据,进程数：%d", proc_num)
    # 请求头
    # 默认为keep-alive，即连接一次，传输多次，然而在多次访问后不能结束并回到连接池中，导致不能产生新的连接，具体表现为err_callback报错Max retries exceed with URL
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/100.0.4896.75 Safari/537.36',
        'Content-Type': 'application/json',
        'Connection': 'close'
    }
    url_suffix = ''
    for _ in systemWeight.split():
        url_suffix += 'systemWeight=' + _ if not url_suffix else '&systemWeight=' + _
    request_url = "http://" + sw_compare_api + "/tuling/mlv/v2/vsPlusCheckResource?" + url_suffix

    df = pd.DataFrame(columns=['样本文件', '与垃圾音库比对的最高得分'])
    pool = multiprocessing.Pool(proc_num)
    pool_tasks = []
    with open(output_file, 'r') as f:
        for f_item in f.read().splitlines():  # f_item 包含样本的声纹数据
            scores = []
            f_item = f_item.split()
            sw1 = f_item[1]
            new_line = [f_item[0]]  # 待新增的数据，首先传入样本文件名
            if sw1 == "task_failed":
                continue
            logger.info("开始获取文件 %s 和 垃圾音库的比对结果，请稍等~" % f_item[0])
            with open(trash_libs_sw, 'r') as g:
                for g_item in g.read().splitlines():  # g_item 包含垃圾音库声纹数据
                    g_item = g_item.split()
                    sw2 = g_item[1]
                    if sw2 != "task_failed":
                        task = pool.apply_async(process_compare, (f_item[0], sw1, g_item[0], sw2, request_url, headers),
                                                callback=collect_data, error_callback=error_callback_func)
                        pool_tasks.append(task)
            [_.wait() for _ in pool_tasks]
            try:
                new_line.append(max(scores))
                df.loc[len(df)] = new_line
            except Exception as e:
                logger.info("警告：%s 文件记录比对得分异常" % f_item[0])
                raise e
    pool.close()
    pool.join()
    logger.info('正在记录声纹比对得分。。。')
    df.to_excel(sw_compare_result, index=False)
    end_time = time.time()  # 记录程序结束时间
    logger.info("本次SW-TOOL程序耗时 %.2f秒" % (end_time - start_time))
