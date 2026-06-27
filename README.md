# FilmRadar 电影推荐系统

FilmRadar 是一个 Web 电影推荐系统。用户登录后可以浏览电影、提交评分，并基于评分数据获取个性化推荐；系统提供热门、最新、随机、类型加权和 Spark ALS 协同过滤等推荐策略。

## 功能概览

- 用户注册、登录和管理员账号初始化
- 随机电影评分与用户评分记录管理
- 电影列表、电影详情、管理员电影管理
- 类型加权推荐与 Spark ALS 协同过滤推荐
- 推荐理由、推荐度分布和用户类型偏好可视化
- HDFS 评分同步失败时降级使用 MySQL JDBC 读取评分数据

## 技术栈

- 前端：HTML、CSS、JavaScript、ECharts
- 后端：Node.js、Express、mysql2
- 数据库：MySQL 5.7+
- 推荐算法：Spark 2.4.x ALS、PySpark、pymysql
- 可选大数据组件：Hadoop HDFS

## 运行文件结构

```text
FilmRadar/
├── server/
│   ├── server.js
│   ├── package.json
│   ├── package-lock.json
│   ├── public/
│   │   ├── index.html
│   │   └── login.html
│   └── spark/
│       └── als_recommend_new.py
├── sql/
│   ├── schema.sql
│   ├── seed_users.sql
│   ├── seed_movies.sql
│   └── seed_ratings.sql
├── .env.example
└── README.md
```

仓库只保留系统启动和运行所需的源码、配置示例、依赖锁定文件和数据库脚本；课程报告、压缩包、重复 SQL 说明、旧版 Spark 脚本、可选测试脚本等交付或辅助材料未纳入。

## 快速启动

1. 创建数据库并导入数据：

```bash
mysql -u root -p < sql/schema.sql
mysql -u root -p movie_recommend < sql/seed_users.sql
mysql -u root -p movie_recommend < sql/seed_movies.sql
mysql -u root -p movie_recommend < sql/seed_ratings.sql
```

2. 配置环境变量：

```bash
cp .env.example .env
# 按本机 MySQL / Spark / HDFS 配置修改 .env
```

3. 安装并启动后端：

```bash
cd server
npm install
npm start
```

4. 访问系统：

```text
http://localhost:3000/login.html
```

测试账号：

| 角色 | 用户名 | 密码 |
| --- | --- | --- |
| 普通用户 | testuser1 ~ testuser20 | 123456 |
| 管理员 | admin | 123456 |

## Spark ALS

系统默认使用 `server/spark/als_recommend_new.py`。后端触发协同过滤推荐时，会先尝试把 MySQL 评分同步到 HDFS，再运行 Spark ALS；如果 HDFS 数据不可用，脚本会自动降级为 MySQL JDBC 读取。

常用环境变量见 `.env.example`：

```bash
SPARK_SUBMIT=spark-submit
SPARK_MASTER=local[*]
PYSPARK_PYTHON=python3
HDFS_RATINGS=/user/fsl/movie_recommend/ratings.txt
ADMIN_PASSWORD=123456
```

## 说明

本项目为课程设计演示版本。默认数据库账号、测试账号和 MD5 密码存储方式只适合本地学习与展示；生产环境应改用更安全的密钥管理、密码哈希和权限控制方案。
