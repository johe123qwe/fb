import warnings
import click
import pandas as pd
import threading

warnings.simplefilter(action="ignore", category=FutureWarning)
from datetime import datetime
import os
import random
import sqlite3
import time
from zoneinfo import ZoneInfo
import gspread
import os
import getfb_posts
# from facebook_scraper import *
from google.oauth2 import service_account
from googleapiclient.discovery import build
from skpy import Skype
import group_info_config

'''安装
pip install -q -U google-generativeai
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
pip install gspread click
'''

'''更新日志
20250318 改为批量上传数据
20240419 增加获取抖音的数据
'''


#### 配置区域start ####

# 间隔时间
time_a = 9
time_b = 13
#### 配置区域end ####
# Skype 账号、密码
sk_username = group_info_config.SK_USERNAME
sk_password = group_info_config.SK_PASSWORD
groupid = group_info_config.GROUP_ID

# 读取谷歌表格， token.json 获取为 google sheet API
SERVICE_ACCOUNT_FILE = "token.json"
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
creds = None
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

service_account_file = os.path.abspath("token.json")
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()



sqlilt_db = os.path.join(os.path.dirname(__file__), "sqlite3", "de_post_group.db")

tday = datetime.now()  # 当前时间
# tday_ = tday.strftime("%Y-%m-%d %H:%M:%S")  # 当前日期时间
tday__ = tday.strftime("%Y-%m-%d")  # 当前日期时间

def time_now(timezone):
    """获取当前日期"""
    time_now = datetime.now(ZoneInfo(timezone)).strftime("%Y-%m-%d %H:%M:%S")
    return time_now


emoji_list = [
    "☘️",
    "🥕",
    "🍀",
    "🚗",
    "🍊",
    "🍋",
    "🍍",
    "🥭",
    "🍎",
    "🍓",
    "🍒",
    "🍆",
    "🫒",
    "🍅",
    "🍔",
    "🗓️",
    "🟢",
    "🔵",
    "😊",
    "😬",
    "🥺",
    "👀",
    "🫑",
    "🌸",
    "🏵️",
    "🌺",
    "🌻",
    "🚗",
    "🚕",
    "🚙",
    "👍",
    "🌟",
    "☀️",
    "🥦",
    "🥬",
    "🥒",
    "🌽",
    "🏆",
    "🎁",
    "💖",
    "💝",
    "🧡",
    "✅",
    "💐",
    "🥀",
]


def gspread_clint():
    """Connects to a Google Sheet."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=scope
    )
    client = gspread.authorize(creds)
    return client

def list_sheet(sheet_key):
    client = gspread_clint()
    try:
        sheet = client.open_by_key(sheet_key).worksheets()
        return sheet
    except gspread.exceptions.WorksheetNotFound:
        return None

def get_id_name(id) -> dict:
    sheet_names = list_sheet(id)
    id_name_dic = {item.title : item.id for item in sheet_names}
    return id_name_dic

def get_data(sheet_name, sheet_range, sheet_id):
    """获取表格数据sheet_name, sheet_range, sheet_id"""
    info_data = ""
    SCOPES = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    sheet = service.spreadsheets()
    name_range = "{}!{}".format(sheet_name, sheet_range)
    try:
        result = sheet.values().get(spreadsheetId=sheet_id, range=name_range).execute()
        info_data = result.get("values", [])
        df = pd.DataFrame(info_data)
        return df, info_data
    except Exception as e:
        info = "读取工作表数据 {} {} 失败,请检查. {}".format(sheet_id, sheet_name, e)
        print(60, e)
        return 0


def get_group_id(value, column=2):
    """获取小组ID"""
    group_id = []
    for row in range(0, value.shape[0]):
        # print(96, value.iat[row, column])
        group_id.append(value.iat[row, column])
    return group_id


def get_fb_group_member(group_id, cookie):
    """获取小组人数"""
    #  cookies='/Users/xiaoyu/develop/pyscript/de_post/cookie_files/facebook_cookiesde02.txt'
    try:
        ginfo = get_group_info(
            group_id,
            cookies=cookie,
        )
        return ginfo["members"]
    except Exception as e:
        print(144, e)
        return -1


def get_fb_group_member2(group_id):
    """获取小组人数, 不使用 cookie"""
    try:
        ginfo = get_group_info(
            group_id,
        )
        return ginfo["members"]
    except Exception as e:
        print(155, e)
        return -1


def getmembers(group_ids):
    """获取小组人数,输出为列表"""
    members = []
    for id in group_ids:
        row = []
        if id != None:
            nums = get_fb_group_member(id, "./cookie_files/facebook_cookiesde02.txt")
            try_times = 0
            while nums == -1:
                print("stop10 {}".format(id))
                try_times += 1
                time.sleep(10)
                if 5 >= try_times > 1:
                    nums = get_fb_group_member2(id)
                elif 7 >= try_times > 5:
                    nums = get_fb_group_member(
                        id, "./cookie_files/facebook_cookiesen02.txt"
                    )
                elif 8 >= try_times > 7:
                    nums = get_fb_group_member(
                        id, "./cookie_files/facebook_cookiesen03.txt"
                    )
                elif try_times > 8:
                    nums = -1
            else:
                print("小组", nums, id)
                row.append(nums)
                time.sleep(5)
        elif id == None:
            row.append(None)
        members.append(row)
    return members


def send_telegram(msg):
    import telegram

    tel_group_id = "175568461"
    token = "783640552:AAGykkJBhYGfRD_Qj8LKXGECuX9hkFLsEXc"

    bot = telegram.Bot(token)
    try:
        bot.send_message(chat_id=tel_group_id, text=msg)
    except Exception as e:
        logger.error("Telegram sends information failed {} {}".format(msg, e))

def get_tk_posts(group_id):
    '''获取抖音数据
    粉丝、关注、好友、点赞'''
    count = 0
    result = getfb_posts.get_tiktok(group_id)
    while result[0] == None and count < 3:
        count += 1
        time.sleep(10)
        result = getfb_posts.get_tiktok(group_id)
    return result

# 写一个读取文件函数
def read_file():
    '''读取一个文件文件'''

def get_fb_posts(group_id):
    result = getfb_posts.get_fb_posts_local(group_id)
    print(group_id, "发帖数", result)
    trys = 0
    while result[0] == "toomany":
        trys += 1
        if trys < 10:
            result = getfb_posts.get_fb_posts_local(group_id)
            print(group_id, "发帖数", result)
            time.sleep(4)
        else:
            # send_telegram("小组 {} 今日发帖数获取失败, {}".format(group_id, result[1]))
            break
    return result[0], result[1]
    # api = CrawlingAPI({ 'token': 'EHNKnzONlTCNVdD6owxB9Q' })
    # response = api.get('https://www.facebook.com/groups/{}/about'.format(group_id))
    # while response['status_code'] == 200:
    #     text = response['body'].decode("utf-8")
    #     result = re.search(r'"number_of_posts_in_last_day":(.*?),', text)
    #     if result:
    #         return result.group(1)
    #     else:
    #         return 0
    # else:
    #     time.sleep(3)
    #     get_fb_posts(group_id)


def getposts(group_ids):
    """获取小组当日帖子数和总帖子数,输出为列表"""
    posts_all = []
    member_all = []
    for id in group_ids:
        posts = []
        members = []
        time.sleep(10)
        if id != None:
            result = get_fb_posts(id)
            nums = result[0]
            posts.append(nums)
            members.append(result[1])
        elif id == None:
            posts.append(None)
            members.append(None)
        posts_all.append(posts)
        member_all.append(members)
    return posts_all, member_all


def upload_data(result, sheet_range, update_sheet_id):
    """上传数据"""
    body = {"values": result}

    # 使用 update 参数写入数据
    sheet.values().update(
        spreadsheetId=update_sheet_id,
        range=sheet_range,
        valueInputOption="USER_ENTERED",
        body=body,
    ).execute()

def upload_data_batch(result, sheet_name, sheet_range, update_sheet_id):
    """批量上传数据"""
    client = gspread_clint()
    sheet = client.open_by_key(update_sheet_id).worksheet(sheet_name)
    sheet.batch_update([
        {
            'range': sheet_range,
            'values': result
        }
    ])

def input_database(group_info, max_retries=5, base_delay=0.1):
    """将爬取的帖子写入数据库，带重试机制"""
    
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(sqlilt_db, timeout=30)  # 设置30秒超时
            c = conn.cursor()
            
            # 开启WAL模式以提高并发性能
            c.execute("PRAGMA journal_mode=WAL")
            
            c.execute(
                """create table if not exists groups_info(
                    id INTEGER PRIMARY KEY, 
                    姓名 text, 
                    小组名字 text, 
                    小组ID text, 
                    日期 text, 
                    小组总人数 INTEGER, 
                    小组当日帖子数 INTEGER
                )"""
            )
            
            c.executemany(
                "INSERT INTO groups_info(姓名, 小组名字, 小组ID, 日期, 小组总人数, 小组当日帖子数) VALUES(?,?,?,?,?,?)",
                group_info,
            )
            
            conn.commit()
            conn.close()
            print(f"数据库操作成功，尝试次数: {attempt + 1}")
            return  # 成功则退出函数
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                if attempt < max_retries - 1:
                    # 指数退避 + 随机延迟
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    print(f"数据库被锁定，第{attempt + 1}次尝试失败，{delay:.2f}秒后重试...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"重试{max_retries}次后仍然失败")
                    raise
            else:
                # 其他类型的错误直接抛出
                raise
        except Exception as e:
            print(f"发生其他错误: {e}")
            raise
        finally:
            # 确保连接被关闭
            try:
                if 'conn' in locals():
                    conn.close()
            except:
                pass

def tk_input_database(group_info):
    """将爬取的tiktok数据写入数据库"""
    conn = sqlite3.connect(sqlilt_db)
    c = conn.cursor()

    # c.execute("""drop table if exists all_posts_info""")
    c.execute(
        """create table if not exists tiktok_info(id INTEGER PRIMARY KEY, 姓名 text, 主页id text, 日期 text, 粉丝数 INTEGER, 关注数 INTEGER, 好友数 INTEGER, 点赞数 INTEGER)"""
    )
    # print(232, group_info)
    c.executemany(
        "INSERT INTO tiktok_info(姓名, 主页id, 日期, 粉丝数, 关注数, 好友数, 点赞数) VALUES(?,?,?,?,?,?,?)",
        group_info,
    )
    conn.commit()
    conn.close()


def db_insert(group_info, tday, members, posts):
    """插入数据库的数据"""
    infos = []
    for row in range(0, group_info.shape[0]):
        group_row = []
        # print(96, value.iat[row, 2])
        group_row.append(group_info.iat[row, 0]),
        group_row.append(group_info.iat[row, 1]),
        group_row.append(group_info.iat[row, 2]),
        group_row.append(tday),
        group_row.append(members[row][0]),
        group_row.append(posts[row][0]),
        infos.append(group_row)
    return infos


def get_dic(value):
    """生成小组id和人数的字典"""
    dic = {}
    for row in range(0, len(value)):
        dic[value[row][0]] = value[row][3]
    return dic


def query_database(vauledate):
    """查询小组增长人数"""
    conn = sqlite3.connect(sqlilt_db)
    c = conn.cursor()

    c.execute(
        '''SELECT "小组ID", date("日期") AS "日期", MAX("日期") AS "最后时间", "小组总人数" FROM groups_info WHERE date("日期") = date({}) GROUP BY "小组ID"'''.format(
            vauledate
        )
    )

    query_result = c.fetchall()  # 当天最近一次的小组人数
    t_dic = get_dic(query_result)
    # print(346, t_dic)
    return t_dic

def query_database_tk(vauledate):
    """查询tk小组增长人数"""
    conn = sqlite3.connect(sqlilt_db)
    c = conn.cursor()

    c.execute(
        '''SELECT "主页id", date("日期") AS "日期", MAX("日期") AS "最后时间", "粉丝数" FROM tiktok_info WHERE date("日期") = date({}) GROUP BY "主页id"'''.format(
            vauledate
        )
    )

    query_result = c.fetchall()  # 当天最近一次的小组人数
    t_dic = get_dic(query_result)
    # print(346, t_dic)
    return t_dic

def query_database2_tk(column_name, vauledate):
    """查询tiktok发帖数"""
    conn = sqlite3.connect(sqlilt_db)
    c = conn.cursor()

    c.execute(
        '''SELECT "主页id", date("日期") AS "日期", MAX("日期") AS "最后时间", "{}" FROM tiktok_info WHERE date("日期") = date({}) GROUP BY "主页id"'''.format(
            column_name, vauledate
        )
    )

    query_result = c.fetchall()  # 当天最近一次的小组人数
    t_dic = get_dic(query_result)
    return t_dic

def query_database2(vauledate):
    """查询小组发帖数"""
    conn = sqlite3.connect(sqlilt_db)
    c = conn.cursor()

    c.execute(
        '''SELECT "小组ID", date("日期") AS "日期", MAX("日期") AS "最后时间", "小组当日帖子数" FROM groups_info WHERE date("日期") = date({}) GROUP BY "小组ID"'''.format(
            vauledate
        )
    )

    query_result = c.fetchall()  # 当天最近一次的小组人数
    t_dic = get_dic(query_result)
    return t_dic


def count_group(t_dic, b_dic):
    """计算当天与前一天的小组人数差，生成字典
    t_dic: 当天的小组人数
    b_dic: 前一天的小组人数"""
    dic = {}
    for key, value in t_dic.items():
        if (
            str(t_dic.get(key)).replace(",", "").isdigit()
            and str(b_dic.get(key)).replace(",", "").isdigit()
        ):
            # print('当天小组人数', t_dic.get(key), key)
            # print('前一天小组人数', b_dic.get(key), key)
            increase = int(str(t_dic.get(key)).replace(",", "")) - int(
                str(b_dic.get(key)).replace(",", "")
            )
            dic[key] = increase
    return dic


def get_upload_group_increase(ids, dic):
    """获取用于上传表格的小组增长人数"""
    nums = []
    for id in range(0, len(ids)):
        row = []
        row.append(dic.get(ids[id], 0))
        nums.append(row)
    return nums


def update_time(sheet_name, sheet_id, timezone, sheet_range="E1", content_title="总人数"):
    """更新时间"""
    current_time = []
    upoad_current_time = []
    time_now = datetime.now(ZoneInfo(timezone)).strftime("%Y-%m-%d %H:%M:%S")
    update_time = time_now + " {}".format(content_title)
    current_time.append(update_time)
    upoad_current_time.append(current_time)
    body6 = {"values": upoad_current_time}
    # 使用 update 参数写入数据
    sheet.values().update(
        spreadsheetId=sheet_id,
        range="{}!{}".format(sheet_name, sheet_range),
        valueInputOption="USER_ENTERED",
        body=body6,
    ).execute()


def send_sk(info, SK_ID):
    """发送 skype 信息,
    rich=True 为富文本，可以@用户，但是开启rich后
    使用的表情不能为skype自带的表情，可以使用windows或mac系统
    自带的表情。"""
    try:
        sk = Skype(sk_username, sk_password)
        ch = sk.chats[SK_ID]  # 给小组发信息
        ch.sendMsg(info, rich=False)
        return 1
    except Exception as e:
        return 0


def group_sort_tk(sheet_name, sheet_id, send_type=7):
    """小组来人排名"""
    result = get_data(sheet_name, "B4:J", sheet_id)
    df = result[0]
    # print(df, 498)
    # 提取姓氏部分
    df[0] = df[0].str.split('-').str[1].str[:-3]

    # 获取不同的人名
    unique_names = df[0].unique()
    print("数据中的人名有:", unique_names)

    name_count_dic = {}
    for name in unique_names:
        total = pd.to_numeric(df[df[0].str.contains(name)][int(send_type)], errors='coerce').sum()
        name_count_dic[name] = total
    print(name_count_dic)

    # 根据字典的值进行排序
    sorted_data = sorted(name_count_dic.items(), key=lambda x: x[1], reverse=True)
    top_num = sorted_data[:20]
    return top_num

def group_sort(sheet_name, sheet_id):
    """小组来人排名"""
    result = get_data(sheet_name, "B4:F", sheet_id)
    sorted_list = sorted(result[1], key=lambda x: int(x[4]))
    nlist = sorted_list[-10:]
    for index in range(-len(sorted_list), -10):
        if sorted_list[-10][4] == sorted_list[index][4]:
            nlist.insert(0, sorted_list[index])
    nlist.reverse()
    return nlist, result[1]


def skype_info(li):
    content = "德语自建小组 {} 报告\n".format(tday__)
    for index in range(len(li)):
        result = "{}{} {} 新增 {}".format(
            random.sample(emoji_list, 1)[0], li[index][0], li[index][1], li[index][3]
        )
        content += result + "\n"
    return content

def get_titok_post_info(group_ids):
    """获取小组当日帖子数和总帖子数,输出为列表"""
    fans_all = []
    follows_all = []
    friends_all = []
    likes_all = []
    for id in group_ids:
        fans = []
        follows = []
        friends = []
        likes = []
        time.sleep(2)
        print(id, 478)
        if id != None:
            result = get_tk_posts(id)
            if result != None:
                fans.append(result[0])
                follows.append(result[1])
                friends.append(result[2])
                likes.append(result[3])
            else:
                fans.append(None)
                follows.append(None)
                friends.append(None)
                likes.append(None)
        elif id == None:
            fans.append(None)
            follows.append(None)
            friends.append(None)
            likes.append(None)
        fans_all.append(fans)
        follows_all.append(follows)
        friends_all.append(friends)
        likes_all.append(likes)
    return fans_all, follows_all, friends_all, likes_all

def upload_tk_today(sheet_name, sheet_id, result_all):
    '''上传抖音今日数据'''
    upload_data_list = []
    # print('列表数组长度:', len(result_all[0]))
    for row in range(len(result_all[0])):
        fans = result_all[0][row][0]
        if fans == None:
            fans = 0

        followers = result_all[1][row][0]
        if followers == None:
            followers = 0

        friends = result_all[2][row][0]
        if friends == None:
            friends = 0

        likes = result_all[3][row][0]
        if likes == None:
            likes = 0

        upload_data_list.append([fans, followers, friends, likes])
    # print(upload_data_list, 515)
    upload_data(upload_data_list, "{}!D4:G".format(sheet_name), sheet_id)
    return upload_data_list

def upload_tk_fri_fol_increase(sheet_name, sheet_id, ids):
    '''上传每日增长的好友和关注数量'''
    sheet_columns_followers = ['I', 'L', 'O', 'R', 'U', 'X', 
                           'AA', 'AD', 'AG', 'AJ', 'AM', 
                           'AP', 'AS', 'AV', 'AY', 'BB']

    sheet_columns_friends = ['J', 'M', 'P', 'S', 'V', 'Y', 
                             'AB', 'AE', 'AH', 'AK', 'AN', 
                             'AQ', 'AT', 'AW', 'AZ', 'BC']

    # 查询结果列表，这次不需要包含今天的结果，因为我们计算的是发帖数增长
    dics = {
        "关注数": sheet_columns_followers,
        "好友数": sheet_columns_friends
    }

    # 获取前x天的小组发帖数并添加到列表中
    for i, table_col in dics.items():
        query_results_posts = []
        for j in range(0, len(table_col)):
            query_result = query_database2_tk(i, f"'now', '-{j} day'")
            query_results_posts.append(query_result)

        for j, _ in enumerate(query_results_posts):
            if j < len(query_results_posts) - 1:
                query_result_data = count_group(query_results_posts[j], query_results_posts[j+1])
                posts_increase = get_upload_group_increase(ids, query_result_data)
                time.sleep(4)
                upload_data(posts_increase, f"{sheet_name}!{table_col[j]}4:{table_col[j]}", sheet_id)

def upload_tk_fans_increase(sheet_name, sheet_id, ids):
    '''上传每日增长的粉丝数量'''
    sheet_columns = ['H', 'K', 'N', 'Q', 'T', 'W', 
                     'Z', 'AC', 'AF', 'AI', 
                     'AL', 'AO', 'AR', 'AU', 'AX', 'BA']

    # 查询结果列表，初始化包含今天的查询结果
    query_results = [query_database_tk("'now'")]
    # 获取前10天的小组人数并添加到列表中
    for i in range(1, len(sheet_columns)+1):
        query_result = query_database_tk(f"'now', '-{i} day'")
        query_results.append(query_result)  # 将结果插入列表的开头

    for i in range(1, len(query_results)):
        # 计算前一天与当前天的小组人数增长
        query_result_data = count_group(query_results[i-1], query_results[i])
        increase = get_upload_group_increase(ids, query_result_data)
        # 上传数据到对应的列
        time.sleep(4)
        upload_data(increase, f"{sheet_name}!{sheet_columns[i-1]}4:{sheet_columns[i-1]}", sheet_id)

def job_tk(sheet_id, sheet_name, sheet_range="B4:C"):
    '''抖音数据处理'''
    sheet_date = get_data(sheet_name, sheet_range, sheet_id)
    result = sheet_date[0]
    ids = get_group_id(result, 1)

    result_all = list(get_titok_post_info(ids))
    print(result_all, 504)

    upload_data_list = upload_tk_today(sheet_name, sheet_id, result_all) # 上传今日数据
    print('今日数据已更新')

    # 插入数据库,姓名，ID，粉丝数，关注数，好友数，点赞数
    db_data = tk_db_insert(result, tday_, upload_data_list)
    tk_input_database(db_data)

    upload_tk_fans_increase(sheet_name, sheet_id, ids)
    print('粉丝数据已更新')
    upload_tk_fri_fol_increase(sheet_name, sheet_id, ids)
    print('好友和关注数据已更新')

    update_time(sheet_name, sheet_id, "D1", "粉丝总数") # 更新表格时间

def tk_db_insert(group_info, tday, datas):
    """插入数据库的数据"""
    infos = []
    for row in range(0, group_info.shape[0]):
        group_row = []
        # print(96, value.iat[row, 2])
        group_row.append(group_info.iat[row, 0]),
        group_row.append(group_info.iat[row, 1]),
        group_row.append(tday),
        group_row.append(datas[row][0]),
        group_row.append(datas[row][1]),
        group_row.append(datas[row][2]),
        group_row.append(datas[row][3]),
        infos.append(group_row)
    # print(infos, 531)
    return infos

def process_fb_chunk(sheet_id, sheet_name, ids_chunk, result_chunk):
    """处理Facebook数据的子集"""
    # 获取帖子和成员数据
    result_post_mem_all = getposts(ids_chunk)
    result_posts = result_post_mem_all[0]
    result_members = result_post_mem_all[1]
    
    # 准备数据库插入数据
    db_data = db_insert(result_chunk, tday_, result_members, result_posts)
    
    # 返回处理结果
    return {
        'posts': result_posts,
        'members': result_members,
        'db_data': db_data
    }

def job_fb(sheet_id, sheet_name, timezone, sheet_range="B4:D", chunk_size=4000):
    sheet_date = get_data(sheet_name, sheet_range, sheet_id)
    result = sheet_date[0]
    ids = get_group_id(result)
    
    # 检查行数
    row_count = result.shape[0]
    if row_count > chunk_size:
        print(f"行数超过{chunk_size}（当前为{row_count}），使用多线程处理")
        
        # 计算需要的线程数
        num_chunks = (row_count + chunk_size - 1) // chunk_size  # 向上取整
        
        # 创建线程和结果容器
        threads = []
        results = [None] * num_chunks
        
        # 分割数据并创建线程
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, row_count)
            
            # 创建DataFrame子集
            result_chunk = result.iloc[start_idx:end_idx].copy()
            ids_chunk = ids[start_idx:end_idx]
            
            # 创建并启动线程
            thread = threading.Thread(
                target=lambda idx=i, r_chunk=result_chunk, i_chunk=ids_chunk: results.__setitem__(idx, process_fb_chunk(sheet_id, sheet_name, i_chunk, r_chunk))
            )
            threads.append(thread)
            thread.start()
            print(f"启动线程 {i+1}/{num_chunks}, 处理行 {start_idx+1}-{end_idx}")
        
        # 等待所有线程完成
        for i, thread in enumerate(threads):
            thread.join()
            print(f"线程 {i+1}/{num_chunks} 已完成")
        
        # 合并结果
        all_posts = []
        all_members = []
        all_db_data = []
        
        for result in results:
            print(771, result['posts'], result['members'], result['db_data'])
            all_posts.extend(result['posts'])
            all_members.extend(result['members'])
            all_db_data.extend(result['db_data'])
        
        # 插入数据库
        input_database(all_db_data)
        # 上传成员数据
        upload_data(all_members, "{}!E4:E".format(sheet_name), sheet_id)
        
    else:
        # 原有的单线程处理逻辑
        result_post_mem_all = getposts(ids)
        result_posts = result_post_mem_all[0]
        result_members = result_post_mem_all[1]
        
        db_data = db_insert(result, tday_, result_members, result_posts)
        input_database(db_data)

        upload_data(result_members, "{}!E4:E".format(sheet_name), sheet_id)

    
    # 查询结果列表，初始化包含今天的查询结果
    query_results = [query_database("'now'")]

    # 获取前10天的小组人数并添加到列表中
    for i in range(1, 34):
        query_result = query_database(f"'now', '-{i} day'")
        query_results.append(query_result)  # 将结果插入列表的开头

    # 遍历查询结果，计算增长并上传数据
    datas = []
    for i in range(1, len(query_results)):
        # 计算前一天与当前天的小组人数增长
        query_result_data = count_group(query_results[i-1], query_results[i])
        increase = get_upload_group_increase(ids, query_result_data)
        datas.append(increase)

    # 查询结果列表，这次不需要包含今天的结果，因为我们计算的是发帖数增长
    query_results_posts = []

    # 获取前x天的小组发帖数并添加到列表中
    for i in range(0, 33):
        query_result = query_database2(f"'now', '-{i} day'")
        query_results_posts.append(query_result)

    # 遍历查询结果，计算增长并上传数据
    group_posts = []
    for i, result in enumerate(query_results_posts):
        posts_increase = get_upload_group_increase(ids, result)
        group_posts.append(posts_increase)

    # 批量上传
    merged_list = []
    # 假设每个组的行数相同
    num_rows = len(datas[0])
    for r in range(num_rows):
        row = []
        # 对于每个组（假设组数一致）
        for group in range(len(datas)):
            row.append(datas[group][r][0])
            row.append(group_posts[group][r][0])
        merged_list.append(row)
    upload_data_batch(merged_list, sheet_name, "F4:BS", sheet_id)


def get_sheets(sheet_id, sheet_range="A2:C"):
    sheets = get_data("info", sheet_range, sheet_id)
    return sheets


@click.command()
@click.option("-s", "--sheet_id", help="更新的表格 ID")
@click.option("-p", "--platform", help="平台名称")
@click.option("-r", "--sheet_range", default="A2:C", help="sheet范围")
@click.option("-c", "--chunk_size", default=4000, type=int, help="多线程处理时每个线程处理的最大行数")
@click.option("-t", "--timezone", default="Europe/Berlin", help="时区")
def main(sheet_id, platform, sheet_range, chunk_size, timezone):
    global tday_
    tday_ = time_now(timezone)
    print("当前时间:", tday_)
    if platform == "fb":
        sheets_name = get_sheets(sheet_id, sheet_range)
        for sheet in sheets_name[0][0]:
            job_fb(sheet_id, sheet, chunk_size=chunk_size, timezone=timezone)
            update_time(sheet, sheet_id, timezone)
    elif platform == "tiktok":
        sheets_name = get_sheets(sheet_id)
        for sheet in sheets_name[0][0]:
            job_tk(sheet_id, sheet, "B4:C")

if __name__ == "__main__":
    main()
