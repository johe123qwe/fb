import warnings
import click
import pandas as pd
import group_info_config

warnings.simplefilter(action="ignore", category=FutureWarning)
import datetime
import os
import random
import sqlite3
import time


import getfb_posts
from facebook_scraper import *
from google.oauth2 import service_account
from googleapiclient.discovery import build
from skpy import Skype

# pip install facebook-scraper
'''
20241110 添加乌克兰语专页数据'''

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


service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

# 更新的表格 ID
# update_sheet_id = "1kexU2LSwcIFW6WPiIuJYKM3MqSu11dfKjEWUStJbMzY" # 技术部专页
# update_sheet_id = "152FvJNxGyjfH9EHjszPIxtSHcHgj-9eRk2M2QH2ofr8" # 技术部专页
# update_sheet_id = "1eDAjCkU1iZwnC6Cjw1VHJ66MH0olLjKls3GtVeK2H14"  # 测试

sqlilt_db = os.path.join(os.path.dirname(__file__), "de_post_group.db")

tday = datetime.now()  # 当前时间
tday_ = tday.strftime("%Y-%m-%d %H:%M:%S")  # 当前日期时间
tday__ = tday.strftime("%Y-%m-%d")  # 当前日期时间

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
]


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


def get_group_id(value):
    """获取小组ID"""
    group_id = []
    for row in range(0, value.shape[0]):
        # print(96, value.iat[row, 2])
        group_id.append(value.iat[row, 3])
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
    #  cookies='/Users/xiaoyu/develop/pyscript/de_post/cookie_files/facebook_cookiesde02.txt'
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
                time.sleep(2)
        elif id == None:
            row.append(None)
        members.append(row)
    return members

'''
def send_telegram(msg):
    import telegram

    tel_group_id = "175568461"
    token = "783640552:AAGykkJBhYGfRD_Qj8LKXGECuX9hkFLsEXc"

    bot = telegram.Bot(token)
    try:
        bot.send_message(chat_id=tel_group_id, text=msg)
    except Exception as e:
        logger.error("telegram 发送信息失败 {} {}".format(msg, e))
'''

def get_fb_posts(group_id):
    result = getfb_posts.get_fbpage(group_id)
    trys = 0
    while result[0] == 'toomany':
        trys += 1
        if trys < 5:
            result = getfb_posts.get_fbpage(group_id)
        else:
            # send_telegram("facebook 爬取帖子数失败 {}, {}".format(group_id, result[1]))
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
            posts.append(result[0])
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


def input_database(group_info):
    """将爬取的帖子写入数据库"""
    conn = sqlite3.connect(sqlilt_db)
    c = conn.cursor()

    # c.execute("""drop table if exists all_zhuanye_info""")
    c.execute(
        """create table if not exists all_zhuanye_info(id INTEGER PRIMARY KEY, 语言 text, 专页管理员 text, 专页名字 text, 专页链接 text, 粉丝总数 INTEGER, 点赞数 INTEGER, 日期 text)"""
    )
    # print(232, group_info)
    c.executemany(
        "INSERT INTO all_zhuanye_info(语言, 专页管理员, 专页名字, 专页链接, 粉丝总数, 点赞数, 日期) VALUES(?,?,?,?,?,?,?)",
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
        group_row.append(group_info.iat[row, 3]),
        group_row.append(members[row][0]),
        group_row.append(posts[row][0]),
        group_row.append(tday),
        infos.append(group_row)
    return infos


def get_dic(value):
    """生成小组id和人数的字典"""
    dic = {}
    for row in range(0, len(value)):
        dic[value[row][0]] = value[row][3]
    return dic


def query_database(vauledate):
    """查询粉丝数"""
    conn = sqlite3.connect(sqlilt_db)
    c = conn.cursor()

    c.execute(
        '''SELECT "专页链接", date("日期") AS "日期", MAX("日期") AS "最后时间", "粉丝总数" FROM all_zhuanye_info WHERE date("日期") = date({}) GROUP BY "专页链接"'''.format(
            vauledate
        )
    )

    query_result = c.fetchall()  # 当天最近一次的小组人数
    t_dic = get_dic(query_result)
    return t_dic


def query_database2(vauledate):
    """查询点赞数"""
    conn = sqlite3.connect(sqlilt_db)
    c = conn.cursor()

    c.execute(
        '''SELECT "专页链接", date("日期") AS "日期", MAX("日期") AS "最后时间", "点赞数" FROM all_zhuanye_info WHERE date("日期") = date({}) GROUP BY "专页链接"'''.format(
            vauledate
        )
    )

    query_result = c.fetchall()  # 当天最近一次的小组人数
    t_dic = get_dic(query_result)
    return t_dic


def count_group(t_dic, b_dic):
    """计算当天与前一天的小组人数差，生成字典"""
    dic = {}
    for key, value in t_dic.items():
        if str(t_dic.get(key)).replace(',', "").isdigit() and str(b_dic.get(key)).replace(',', "").isdigit():
            increase = int(str(t_dic.get(key)).replace(',', "")) - int(str(b_dic.get(key)).replace(',', ""))
            dic[key] = increase
    return dic


def get_upload_group_increase(ids, dic):
    """获取用于上传表格的点赞增长人数"""
    nums = []
    for id in range(0, len(ids)):
        row = []
        row.append(dic.get(ids[id], 0))
        nums.append(row)
    return nums


def update_time(sheet_name, update_sheet_id):
    """更新时间"""
    current_time = []
    upoad_current_time = []
    time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    update_time = time_now
    current_time.append(update_time)
    upoad_current_time.append(current_time)
    body6 = {"values": upoad_current_time}
    # 使用 update 参数写入数据
    sheet.values().update(
        spreadsheetId=update_sheet_id,
        range="{}!E1".format(sheet_name),
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


def group_sort(sheet_name, sheet_id):
    """小组来人排名"""
    result = get_data(sheet_name, "B4:F7", sheet_id)
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


def job(sheet_id, sheet_name):
    sheet_date = get_data(sheet_name, "A4:D", sheet_id)
    result = sheet_date[0]
    ids = get_group_id(result)
    # result_members = getmembers(ids)
    result_post_mem_all = getposts(ids)
    result_posts = result_post_mem_all[0]
    result_members = result_post_mem_all[1]
    upload_data(result_members, "{}!E4:E".format(sheet_name), sheet_id)
    upload_data(result_posts, "{}!F4:F".format(sheet_name), sheet_id)
    db_data = db_insert(result, tday_, result_members, result_posts)
    input_database(db_data)

    today_result = query_database("'now'")  # 今天最近一次的小组人数
    before_result = query_database("'now', '-1 day'")  # 前一天最近一次的小组人数
    before1_result = query_database("'now', '-2 day'")  # 前2天最近一次的小组人数
    before2_result = query_database("'now', '-3 day'")  # 前3天最近一次的小组人数
    before3_result = query_database("'now', '-4 day'")  # 前4天最近一次的小组人数
    before4_result = query_database("'now', '-5 day'")  # 前5天最近一次的小组人数
    before5_result = query_database("'now', '-6 day'")  # 前6天最近一次的小组人数
    before6_result = query_database("'now', '-7 day'")  # 前7天最近一次的小组人数

    query_result_data = count_group(today_result, before_result)
    increase = get_upload_group_increase(ids, query_result_data)
    upload_data(increase, "{}!G4:G".format(sheet_name), sheet_id)

    query_result_data2 = count_group(before_result, before1_result)
    increase2 = get_upload_group_increase(ids, query_result_data2)
    upload_data(increase2, "{}!I4:I".format(sheet_name), sheet_id)

    query_result_data3 = count_group(before1_result, before2_result)
    increase3 = get_upload_group_increase(ids, query_result_data3)
    upload_data(increase3, "{}!K4:K".format(sheet_name), sheet_id)

    query_result_data4 = count_group(before2_result, before3_result)
    increase4 = get_upload_group_increase(ids, query_result_data4)
    upload_data(increase4, "{}!M4:M".format(sheet_name), sheet_id)

    query_result_data5 = count_group(before3_result, before4_result)
    increase5 = get_upload_group_increase(ids, query_result_data5)
    upload_data(increase5, "{}!O4:O".format(sheet_name), sheet_id)

    query_result_data6 = count_group(before4_result, before5_result)
    increase6 = get_upload_group_increase(ids, query_result_data6)
    upload_data(increase6, "{}!Q4:Q".format(sheet_name), sheet_id)

    query_result_data7 = count_group(before5_result, before6_result)
    increase7 = get_upload_group_increase(ids, query_result_data7)
    upload_data(increase7, "{}!S4:S".format(sheet_name), sheet_id)

    today_result2 = query_database2("'now'")
    before_result2 = query_database2("'now', '-1 day'")  # 前一天最近一次的点赞数
    before1_result2 = query_database2("'now', '-2 day'")  # 前2天最近一次的点赞数
    before2_result2 = query_database2("'now', '-3 day'")  # 前3天最近一次的点赞数
    before3_result2 = query_database2("'now', '-4 day'")  # 前4天最近一次的点赞数
    before4_result2 = query_database2("'now', '-5 day'")  # 前5天最近一次的点赞数
    before5_result2 = query_database2("'now', '-6 day'")  # 前6天最近一次的点赞数
    # before6_result2 = query_database2("'now', '-7 day'") # 前7天最近一次的点赞数

    query1_result_data2 = count_group(today_result2, before_result2)
    posts_increase1 = get_upload_group_increase(ids, query1_result_data2)
    upload_data(posts_increase1, "{}!H4:H".format(sheet_name), sheet_id)

    query2_result_data2 = count_group(before_result2, before1_result2)
    posts_increase2 = get_upload_group_increase(ids, query2_result_data2)
    upload_data(posts_increase2, "{}!J4:J".format(sheet_name), sheet_id)

    query3_result_data2 = count_group(before1_result2, before2_result2)
    posts_increase3 = get_upload_group_increase(ids, query3_result_data2)
    upload_data(posts_increase3, "{}!L4:L".format(sheet_name), sheet_id)

    query4_result_data2 = count_group(before2_result2, before3_result2)
    posts_increase4 = get_upload_group_increase(ids, query4_result_data2)
    upload_data(posts_increase4, "{}!N4:N".format(sheet_name), sheet_id)

    query5_result_data2 = count_group(before3_result2, before4_result2)
    posts_increase5 = get_upload_group_increase(ids, query5_result_data2)
    upload_data(posts_increase5, "{}!P4:P".format(sheet_name), sheet_id)

    query6_result_data2 = count_group(before4_result2, before3_result2)
    posts_increase6 = get_upload_group_increase(ids, query6_result_data2)
    upload_data(posts_increase6, "{}!R4:R".format(sheet_name), sheet_id)

    query7_result_data2 = count_group(before5_result2, before4_result2)
    posts_increase7 = get_upload_group_increase(ids, query7_result_data2)
    upload_data(posts_increase7, "{}!T4:T".format(sheet_name), sheet_id)
    update_time(sheet_name, sheet_id)


'''
python group_zhuanye.py -s 专页ID -p 专页
'''
@click.command()
@click.option("-s", "--sheet_id", help="表格 ID")
@click.option("-p", "--sheet_name", help="sheet name")
def main(sheet_id, sheet_name):
    job(sheet_id, sheet_name)

if __name__ == "__main__":
    main()