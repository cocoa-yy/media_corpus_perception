import json
import math
import time
import logging
from datetime import datetime
import mysql.connector
from openai import OpenAI
import schedule

# 配置日志（使用绝对路径）
logging.basicConfig(
    filename='/var/log/news_processing.log',  # 服务器上的日志路径
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# OpenAI 客户端配置
client = OpenAI(
    api_key="****",
    base_url="****"
)

# 数据库连接配置（请替换为实际值）
db_config = {
    'host': '****',    # 只写 IP 地址或域名
    'port': ****,                # 端口号单独指定，默认为 3306
    'user': '****',     # 替换为你的 MySQL 用户名
    'password': '****', # 替换为你的 MySQL 密码
    'database': '****'  # 替换为你的数据库名
}

# 系统提示
system_content = """
你是一个事件关键特征评分系统。请根据输入的新闻内容生成 JSON 格式的输出，规则如下：

#### **1. 关键特征评分**
- 每个关键特征评估其相关度分数（0-5）：
  - **0 分**：完全无关。  
  - **1 分**：弱相关。  
  - **3 分**：中等相关。  
  - **5 分**：强相关。  

- **关键特征类别**：
  1. **冲突性**（国际争端、政策对立、经济制裁等）。  
  2. **名人效应**（国家领导人、科技领袖等）。  
  3. **突发性**（安全事故、自然灾害或预告未来节点等）。  
  4. **经济敏感议题**（贸易摩擦、供应链变化等，不关注股市变化）。  
  5. **社会/文化热点**（电影票房、教育政策、消费政策等）。  
  6. **科技突破**（AI技术、航天成就等）。  
  7. **外交动态**（领导人言论、外交冲突、国际会议等，有丑闻或失态提高得分）。  
"""

# 无需评分的标签列表
SKIP_TAGS = {
    'A股盘面直播', '港股动态', '美股动态', 'A股公告速递', '期货市场情报',
    '股指期货', '券商动态', '禽畜期货', '能源类期货', '黄金'
}


# 计算热点等级
def calculate_hotspot_level(scores):
    if not scores:
        return None
    total_score = sum(scores.values())
    return min(5, math.ceil(total_score / 21 * 5))


# 处理单条记录
def process_record(record_id, content, retry_count=0, max_retries=3):
    user_content = content + " Please respond in the format {\"冲突性\": ..., \"名人效应\": ..., \"突发性\": ..., \"经济敏感议题\": ..., \"社会/文化热点\": ..., \"科技突破\": ..., \"外交动态\": ...}"
    try:
        response = client.chat.completions.create(
            model="deepseek-ai/DeepSeek-V3",
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content}
            ],
            response_format={"type": "json_object"}
        )
        scores = json.loads(response.choices[0].message.content)
        hotspot_level = calculate_hotspot_level(scores)
        processed_at = datetime.now()
        return scores, hotspot_level, processed_at, True
    except Exception as e:
        logging.error(f"记录 {record_id} 处理失败 (尝试 {retry_count + 1}/{max_retries}): {e}")
        if retry_count < max_retries - 1:
            time.sleep(2)
            return process_record(record_id, content, retry_count + 1, max_retries)
        return None, None, None, False


# 查询标签
def get_subject_tags(cursor, news_id):
    cursor.execute("SELECT subject_name FROM perception_cls_news_subjects WHERE news_id = %s", (news_id,))
    tags = {row[0] for row in cursor.fetchall()}
    return tags


# 检查是否跳过评分
def should_skip_processing(tags):
    if not tags:
        return False
    return tags.issubset(SKIP_TAGS)


# 处理数据库中的记录
def process_news_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # 优先处理 ctime 最新的记录
        cursor.execute("""
            SELECT id, content 
            FROM perception_cls_news 
            WHERE (hotspot_level IS NULL OR feature_scores IS NULL) 
            ORDER BY ctime DESC 
            LIMIT 10
        """)
        records = cursor.fetchall()

        if not records:
            logging.info("没有待处理的记录")
            return

        logging.info(f"开始处理 {len(records)} 条记录")
        for record in records:
            record_id, content = record
            tags = get_subject_tags(cursor, record_id)

            if should_skip_processing(tags):
                hotspot_level = 0
                feature_scores_json = None
                processed_at = datetime.now()
                cursor.execute("""
                    UPDATE perception_cls_news
                    SET hotspot_level = %s, feature_scores = %s, processed_at = %s
                    WHERE id = %s
                """, (hotspot_level, feature_scores_json, processed_at, record_id))
                logging.info(f"记录 {record_id} 跳过评分: hotspot_level=0, feature_scores=NULL (标签: {tags})")
            else:
                scores, hotspot_level, processed_at, success = process_record(record_id, content)
                if success:
                    feature_scores_json = json.dumps(scores)
                    cursor.execute("""
                        UPDATE perception_cls_news
                        SET hotspot_level = %s, feature_scores = %s, processed_at = %s
                        WHERE id = %s
                    """, (hotspot_level, feature_scores_json, processed_at, record_id))
                    logging.info(f"记录 {record_id} 处理成功: hotspot_level={hotspot_level}, feature_scores={scores}")
                else:
                    cursor.execute("""
                        UPDATE perception_cls_news
                        SET processed_at = NULL
                        WHERE id = %s
                    """, (record_id,))
                    logging.warning(f"记录 {record_id} 处理失败，processed_at 设为 NULL")

            conn.commit()

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


if __name__ == "__main__":
    logging.info("脚本启动，处理所有未处理数据")
    process_news_data()
    run_scheduler()