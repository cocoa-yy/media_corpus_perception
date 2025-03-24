# 媒体语料库建设（感知）

本项目旨在构建媒体语料库的感知部分，主要包含以下三个核心功能：  
- **新闻爬取**：从财联社爬取滚动新闻，每5分钟一次，确保新闻数据的及时性和完整性。  
- **新闻热度计算**：读取新闻内容，按照7个维度的规则计算热度值，并将结果存入数据库。  
- **未来事件抽取**：从滚动新闻中识别并抽取未来事件，形成新的数据表，便于后续分析和预测。  


## 项目功能

### 1. 新闻爬取模块 (`crawler_cls.py`)  
- 定时从财联社（ https://www.cls.cn/telegraph ）获取滚动新闻数据，并存储到MySQL数据库中  
- 新闻内容表：`perception_cls_news`

  | 字段名 | 数据类型 | 描述 |
  |--------|----------|------|
  | `id` | INT | 新闻的唯一标识符（主键） |
  | `ctime` | INT | 新闻发布时间的时间戳 |
  | `content` | TEXT | 新闻内容 |
  | `level` | VARCHAR(10) | 新闻级别（如 "C" 等） |
  | `reading_num` | INT | 新闻的阅读数 |
  | `comment_num` | INT | 新闻的评论数 |
  | `share_num` | INT | 新闻的分享数 |
  | `modified_time` | INT | 新闻最后修改时间的时间戳 |
  | `insert_time` | TIMESTAMP | 数据插入数据库的时间（自动设置） |

- 新闻主题表：`perception_cls_news_subjects`

  | 字段名 | 数据类型 | 描述 |
  |--------|----------|------|
  | `news_id` | INT | 对应的新闻 ID（外键） |
  | `subject_id` | INT | 主题的唯一标识符 |
  | `subject_name` | VARCHAR(255) | 主题名称 |

### 2. 热度计算模块 (`hot_spot_detector.py`)
- 读取新闻数据，按照7个维度（冲突性、名人效应、突发性、经济敏感议题、社会/文化热点、科技突破、外交动态）计算热度值，并更新数据库  
- 新闻内容表：`perception_cls_news`
  | 字段名 | 数据类型 | 描述 |
  |--------|----------|------|
  | `id` | INT | 新闻的唯一标识符（主键） |
  | `ctime` | INT | 新闻发布时间的时间戳 |
  | `content` | TEXT | 新闻内容 |
  | `level` | VARCHAR(10) | 新闻级别（如 "C" 等） |
  | `reading_num` | INT | 新闻的阅读数 |
  | `comment_num` | INT | 新闻的评论数 |
  | `share_num` | INT | 新闻的分享数 |
  | `modified_time` | INT | 新闻最后修改时间的时间戳 |
  | `insert_time` | TIMESTAMP | 数据插入数据库的时间（自动设置） |
  | `hotspot_level` | INT | 热度等级（0-5） |
  | `feature_scores` | JSON | 7个维度的评分（JSON格式） |
  | `processed_at` | TIMESTAMP | 数据处理时间（自动设置） |

### 3. 未来事件抽取模块 (`future_events_analysis.py`)
- 从新闻中识别并抽取符合标准的未来事件，形成新的数据表  
- 未来事件表：`perception_future_events`
  | 字段名 | 数据类型 | 描述 |
  |--------|----------|------|
  | `id` | INT | 事件的唯一标识符（自增主键） |
  | `news_id` | INT | 对应的新闻 ID（外键） |
  | `event_description` | VARCHAR(255) | 事件描述 |
  | `expected_time` | DATETIME | 预计事件发生的时间 |
  | `remarks` | VARCHAR(255) | 备注信息 |
  | `created_at` | TIMESTAMP | 数据创建时间（自动设置） |
  | `probability_of_occurrence` | FLOAT | 事件发生的可能性（0到1） |
  | `theme_categories` | JSON | 主题分类（JSON格式） |
  | `region_categories` | JSON | 地区分类（JSON格式） |

## 更新日志
- 2024年3月13日：实现新闻爬取
- 2024年3月14日：实现新闻热度值计算
- 2024年3月20日：实现未来事件抽取
- 待办
  - 更换为本地算力
  - 集合其他数据源

## 技术栈

- **后端**：Python
- **数据库**：MySQL

## 目录结构
media_corpus_perception/  
├── crawler_cls.py                # 新闻爬取模块  
├── hot_spot_detector.py          # 热度计算模块  
├── future_events_analysis.py    # 未来事件抽取模块  
├── README.md                     # 项目说明文档  
└── requirements.txt              # 项目依赖项  
