import requests
import time
import json
import mysql.connector
from mysql.connector import Error

# MySQL 数据库配置
db_config = {
    'host': '****',  # 只写 IP 地址或域名
    'port': ****,  # 端口号单独指定，默认为 3306
    'user': '****',  # 替换为你的 MySQL 用户名
    'password': '****',  # 替换为你的 MySQL 密码
    'database': '****'  # 替换为你的数据库名
}

headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5",
    "Connection": "keep-alive",
    "Content-Type": "application/json;charset=utf-8",
    "Cookie": "HWWAFSESTIME=1741831055895; HWWAFSESID=d3d40d9c3d38a68939; Hm_lvt_fa5455bb5e9f0f260c32a1d45603ba3e=1741831060; HMACCOUNT=8E3FFE8984C989D8; hasTelegraphNotification=on; hasTelegraphRemind=on; hasTelegraphSound=on; vipNotificationState=on; Hm_lpvt_fa5455bb5e9f0f260c32a1d45603ba3e=1741833474; tfstk=gZ6oaRVkK_R7qkQtyZJ76-VWsdPvF09BgwHpJpLUgE8jy4HRYeqhYEBJ9L8KKkbO5HEWp9n5GNIZeTFWpvvWdpzTWReO2g9BL8itx03W0hxzpb8yYL94sWHUHReOVgWoqTjYBTdthdLJ8pRyTKJ2lhlrUwkyuoxev4lEUwS4mEtK8XRyLxu2Xh0yLp7F0oxp4p-rMnNyP9WV3rPNqcFDbEBDqQYNm5MEKiD9aFWyuvzNG3467g8mLv8J-GZRmgUoCip5LNxOy-klSMS1TIXnuPYCniWPTigu0djcM1dlizkHkTKH_pRmYvSDL_RdUdyZon1V26W5rDDeVTB9tFO0YvOp3OdN_amQfiJyYwO1pPMWzMS1C1pgEVtlgGvc4Lcq_UMtdnrd3XGBantDWtGiUutieeM3mocaAQ-XVFE0mXGBantDWoqm_JOyc3TO.",
    "Host": "www.cls.cn",
    "Referer": "https://www.cls.cn/telegraph",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Microsoft Edge";v="134"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"'
}

url = "https://www.cls.cn/nodeapi/telegraphList"
last_time = int(time.time())  # 初始时间戳


def connect_db():
    try:
        conn = mysql.connector.connect(**db_config)
        return conn
    except Error as e:
        print(f"数据库连接失败: {e}")
        return None


def save_to_db(conn, roll_data):
    cursor = conn.cursor()
    try:
        for entry in roll_data:
            news_data = {
                'id': entry['id'],
                'ctime': entry['ctime'],
                'content': entry['content'],
                'level': entry['level'],
                'reading_num': entry['reading_num'],
                'comment_num': entry['comment_num'],
                'share_num': entry['share_num'],
                'modified_time': entry['modified_time']
            }
            subjects = entry.get('subjects', [])

            news_query = """
                INSERT INTO perception_cls_news (id, ctime, content, level, reading_num, comment_num, share_num, modified_time)
                VALUES (%(id)s, %(ctime)s, %(content)s, %(level)s,%(reading_num)s, %(comment_num)s, %(share_num)s, %(modified_time)s)
                ON DUPLICATE KEY UPDATE
                    ctime = VALUES(ctime),
                    content = VALUES(content),
                    level = VALUES(level),
                    reading_num = VALUES(reading_num),
                    comment_num = VALUES(comment_num),
                    share_num = VALUES(share_num),
                    modified_time = VALUES(modified_time),
                    insert_time = CURRENT_TIMESTAMP
            """
            cursor.execute(news_query, news_data)

            delete_subjects_query = "DELETE FROM perception_cls_news_subjects WHERE news_id = %s"
            cursor.execute(delete_subjects_query, (news_data['id'],))

            if subjects:
                subjects_query = """
                    INSERT INTO perception_cls_news_subjects (news_id, subject_id, subject_name)
                    VALUES (%s, %s, %s)
                """
                for subject in subjects:
                    cursor.execute(subjects_query, (news_data['id'], subject['subject_id'], subject['subject_name']))

        conn.commit()
        print(f"成功保存 {len(roll_data)} 条数据到数据库")
    except Error as e:
        print(f"数据库操作失败: {e}")
        conn.rollback()
    finally:
        cursor.close()


def main():
    global last_time
    while True:
        # 每次请求使用当前时间戳
        last_time = int(time.time())

        params = {
            "app": "CailianpressWeb",
            "category": "",
            "lastTime": str(last_time),
            "os": "web",
            "refresh_type": "1",
            "rn": "20",
            "sv": "8.4.6",
            "sign": "fa815d0472341bb06d8aec7892c30273"
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            json_data = response.json()

            if json_data.get("error") == 0:
                roll_data = json_data["data"]["roll_data"]
                if roll_data:
                    # 调试：打印所有 ctime，检查数据顺序
                    ctimes = [entry["ctime"] for entry in roll_data]
                    print(f"所有 ctime: {ctimes}")

                    # 获取最新的条目（最大 ctime）
                    latest_entry = max(roll_data, key=lambda x: x["ctime"])
                    print(f"请求时间戳: {last_time}")
                    print(f"最新条目 (ctime: {latest_entry['ctime']}): {latest_entry['content']}")

                    conn = connect_db()
                    if conn:
                        save_to_db(conn, roll_data)
                        conn.close()

                    # 不更新 last_time，保持每次用当前时间
                    print(f"更新后的 last_time: {last_time}")
                else:
                    print("无新数据返回")
            else:
                print(f"请求错误: {json_data}")

            time.sleep(300)  # 5 分钟间隔

        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            time.sleep(300)


if __name__ == "__main__":
    main()