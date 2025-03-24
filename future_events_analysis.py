import json
import time
import logging
from datetime import datetime
import mysql.connector
from openai import OpenAI
import schedule

# 配置日志（使用绝对路径，适用于服务器环境）
logging.basicConfig(
    filename='/var/log/future_event_extraction.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# OpenAI 客户端配置
client = OpenAI(
    api_key="****",
    base_url="****"
)

# 数据库连接配置
db_config = {
    'host': '****',
    'port': ****,
    'user': '****',
    'password': '****',
    'database': '****'
}

# 无需评分的标签列表
SKIP_TAGS = {
    'A股盘面直播', '港股动态', '美股动态', 'A股公告速递', '期货市场情报',
    '股指期货', '券商动态', '禽畜期货', '能源类期货', '黄金'
}

# 系统提示模板（不包含示例JSON）
system_content_template = """
你是一个未来事件抽取系统。当前时间为 {current_time}。请根据输入的新闻内容识别并抽取出符合以下标准的未来具体事件：
# 抽取标准
1. **具体事件**：必须满足以下至少一项
   - 有明确执行主体（政府/企业/组织名称）
   - 包含可验证的里程碑节点（如产品发布/政策实施/项目启动）
   - 涉及具体行为动作（签约/发射/投产等）
2. **时间特征**：晚于 {current_time}

# 排除标准（满足任一条件即排除）
- 使用"力争""目标"等模糊承诺的表述
- 持续整年的趋势（如GDP预测/行业展望）
- 无明确时间节点的长期目标

# 输出字段
1. **事件描述**：简要说明事件内容（不超过255个字符）。
2. **预计时间**：事件可能发生的时间，格式为“YYYY-MM-DD HH:MM:SS”（例如“2025-06-01 00:00:00”）。如果具体日期不明确但有月份或季度信息，尽量推断并补全为该时间段的第一天（如“2025年6月”补全为“2025-06-01 00:00:00”）；若完全无法确定时间，则返回“未指明”。仅返回晚于 {current_time} 的事件。
3. **备注**：记录其他需要注意的信息（如时间推断依据、不确定性说明等），不超过255个字符。
4. **发生可能性**：评估事件发生的可能性，范围为0到1（0表示不可能发生，1表示肯定会发生）。
5. **主题分类**：分类到一个或多个主题，返回列表。主题包括政治、外交、经济、军事、法律、交通、体育、科技、环境、社会、工业等。
6. **地区分类**：分类到一个或多个地区，返回列表。地区可以是国家、组合地域或国际组织。

请以 JSON 格式返回这些信息，确保内容简洁、准确。如果没有未来事件，返回空数组 []。
"""

# 示例JSON（作为固定文本）
example_json = """
[
  {
    "event_description": "...",
    "expected_time": "YYYY-MM-DD HH:MM:SS" 或 "未指明",
    "remarks": "...",
    "probability_of_occurrence": 0.8,
    "theme_categories": ["经济", "社会"],
    "region_categories": ["中国"]
  },
  ...
]
"""

# 查询标签
def get_subject_tags(cursor, news_id):
    try:
        cursor.execute("SELECT subject_name FROM perception_cls_news_subjects WHERE news_id = %s", (news_id,))
        tags = {row[0] for row in cursor.fetchall()}
        return tags
    except mysql.connector.Error as e:
        logging.warning(f"查询标签失败 (news_id={news_id}): {e}")
        return set()

# 检查是否跳过处理
def should_skip_processing(tags):
    if not tags:
        return False
    return tags.issubset(SKIP_TAGS)

# 处理单条记录
def process_record(record_id, content, current_time, retry_count=0, max_retries=3):
    system_content = system_content_template.format(current_time=current_time)
    full_system_content = system_content + "\n" + example_json
    user_content = content + " Please respond in the format [{\"event_description\": ..., \"expected_time\": ..., \"remarks\": ..., \"probability_of_occurrence\": ..., \"theme_categories\": ..., \"region_categories\": ...}, ...]，如果没有未来事件，返回空数组 []"
    try:
        response = client.chat.completions.create(
            model="deepseek-ai/DeepSeek-V3",
            messages=[
                {"role": "system", "content": full_system_content},
                {"role": "user", "content": user_content}
            ],
            response_format={"type": "json_object"}
        )
        event_data = json.loads(response.choices[0].message.content)
        return event_data, True
    except Exception as e:
        logging.error(f"记录 {record_id} 处理失败 (尝试 {retry_count + 1}/{max_retries}): {e}")
        if retry_count < max_retries - 1:
            time.sleep(2)
            return process_record(record_id, content, current_time, retry_count + 1, max_retries)
        return None, False

# 检查并创建 perception_future_events 表
def ensure_future_events_table(cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS perception_future_events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                news_id INT NOT NULL,
                event_description VARCHAR(255) NOT NULL,
                expected_time DATETIME,
                remarks VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                probability_of_occurrence FLOAT,
                theme_categories JSON,
                region_categories JSON,
                FOREIGN KEY (news_id) REFERENCES perception_cls_news(id)
            )
        """)
        logging.info("确保 perception_future_events 表存在或已创建")
    except mysql.connector.Error as e:
        logging.error(f"创建 perception_future_events 表失败: {e}")

# 处理堆积任务（批量处理，直到查询数量小于10）
def process_backlog_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # 确保表存在
        ensure_future_events_table(cursor)

        # 获取当前时间
        current_time = datetime.now().strftime("%Y年%m月%d日")
        logging.info(f"当前时间设置为: {current_time}")

        # 获取初始未处理记录总数
        cursor.execute("""
            SELECT COUNT(*) 
            FROM perception_cls_news 
            WHERE future_event_status = 'unprocessed'
        """)
        initial_unprocessed = cursor.fetchone()[0]
        logging.info(f"初始发现 {initial_unprocessed} 条未处理记录，开始批量处理")

        batch_size = 100  # 每批处理100条
        processed_count = 0
        target_count = max(1600, initial_unprocessed)  # 目标处理1600条或更多

        while processed_count < target_count:
            cursor.execute("""
                SELECT id, content 
                FROM perception_cls_news 
                WHERE future_event_status = 'unprocessed' 
                ORDER BY ctime DESC 
                LIMIT %s
            """, (batch_size,))
            records = cursor.fetchall()

            if len(records) < 10:  # 查询数量小于10，结束堆积任务处理
                logging.info(f"查询数量 {len(records)} 小于10，堆积任务处理完成，共处理 {processed_count + len(records)} 条记录")
                if records:  # 处理最后一批少于10条的记录
                    process_batch(cursor, conn, records, current_time)
                    processed_count += len(records)
                break

            logging.info(f"处理批次: {processed_count + 1} - {processed_count + len(records)} / {target_count}")
            process_batch(cursor, conn, records, current_time)
            processed_count += len(records)
            conn.commit()

            # 检查剩余未处理记录数
            cursor.execute("SELECT COUNT(*) FROM perception_cls_news WHERE future_event_status = 'unprocessed'")
            remaining = cursor.fetchone()[0]
            logging.info(f"当前剩余未处理记录: {remaining}")

        logging.info(f"堆积任务处理结束，共处理 {processed_count} 条记录，剩余记录将由定时任务处理")

    except mysql.connector.Error as db_err:
        logging.error(f"数据库错误: {db_err}")
    except Exception as e:
        logging.error(f"未知错误: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# 处理一批记录的辅助函数
def process_batch(cursor, conn, records, current_time):
    for record in records:
        record_id, content = record
        tags = get_subject_tags(cursor, record_id)

        if should_skip_processing(tags):
            cursor.execute("""
                UPDATE perception_cls_news 
                SET future_event_status = 'skipped'
                WHERE id = %s
            """, (record_id,))
            logging.info(f"记录 {record_id} 跳过处理（标签: {tags}），状态更新为 'skipped'")
            conn.commit()
            continue

        event_data, success = process_record(record_id, content, current_time)
        if success:
            if event_data:
                for event in event_data:
                    event_description = event.get("event_description", "")
                    expected_time_str = event.get("expected_time", "未指明")
                    remarks = event.get("remarks", "")
                    probability_of_occurrence = event.get("probability_of_occurrence", 0.0)
                    theme_categories = json.dumps(event.get("theme_categories", []), ensure_ascii=False)
                    region_categories = json.dumps(event.get("region_categories", []), ensure_ascii=False)
                    expected_time = None if expected_time_str == "未指明" else expected_time_str

                    cursor.execute("""
                        INSERT INTO perception_future_events (
                            news_id, 
                            event_description, 
                            expected_time, 
                            remarks,
                            probability_of_occurrence,
                            theme_categories,
                            region_categories
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        record_id,
                        event_description,
                        expected_time,
                        remarks,
                        probability_of_occurrence,
                        theme_categories,
                        region_categories
                    ))

                cursor.execute("""
                    UPDATE perception_cls_news 
                    SET future_event_status = 'has_events'
                    WHERE id = %s
                """, (record_id,))
                logging.info(f"记录 {record_id} 处理成功，发现 {len(event_data)} 个未来事件，状态更新为 'has_events'")
            else:
                cursor.execute("""
                    UPDATE perception_cls_news 
                    SET future_event_status = 'no_events'
                    WHERE id = %s
                """, (record_id,))
                logging.info(f"记录 {record_id} 处理成功，无未来事件，状态更新为 'no_events'")
        else:
            logging.warning(f"记录 {record_id} 处理失败，状态保持 'unprocessed'")

        conn.commit()

# 处理定时任务（每5分钟处理10条）
def process_news_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # 确保表存在
        ensure_future_events_table(cursor)

        # 获取当前时间
        current_time = datetime.now().strftime("%Y年%m月%d日")
        logging.info(f"当前时间设置为: {current_time}")

        # 获取未处理的新闻记录
        cursor.execute("""
            SELECT id, content 
            FROM perception_cls_news 
            WHERE future_event_status = 'unprocessed' 
            ORDER BY ctime DESC 
            LIMIT 10
        """)
        records = cursor.fetchall()

        if not records:
            logging.info("没有待处理的记录")
            return

        logging.info(f"开始处理 {len(records)} 条记录")
        process_batch(cursor, conn, records, current_time)
        logging.info(f"本次处理完成，共处理 {len(records)} 条记录")

    except mysql.connector.Error as db_err:
        logging.error(f"数据库错误: {db_err}")
    except Exception as e:
        logging.error(f"未知错误: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# 定时任务（带重启逻辑）
def run_scheduler():
    while True:
        try:
            schedule.every(5).minutes.do(process_news_data)
            logging.info("定时任务已启动，每5分钟运行一次")
            while True:
                schedule.run_pending()
                time.sleep(60)
        except Exception as e:
            logging.error(f"定时任务崩溃: {e}，将在5秒后重启")
            time.sleep(5)

# 主函数
if __name__ == "__main__":
    logging.info("脚本启动，开始处理堆积任务")
    process_backlog_data()  # 先处理堆积任务
    logging.info("堆积任务处理完毕，进入定时任务模式")
    run_scheduler()         # 然后启动定时任务