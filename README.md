# 质检数据集划分服务

面向质检算法团队的训练/验证集划分 API。接收标注系统导出的 CSV（图片ID、工件号、拍摄批次），
只管理目录信息（不存储图片本体），保证**同一工件或同批拍摄的图片不会同时进入训练集和验证集**。

## 快速开始

```bash
docker compose up --build
```

服务启动后打开交互式 API 文档：**http://localhost:8000/docs**（Swagger UI，可直接试调所有接口）。

SQLite 数据库落在命名卷 `split_data` 中，容器重启/重建后数据不丢。

## 划分规则

- **关联成组**：同工件号或同拍摄批次的图片，沿关联链（连通分量）归为同一组。
- **整组划分**：按组内任一图片 ID 调用 `POST /partitions/assign`，整组指定到 `train` 或 `val`；
  已指定的组不可改动（返回 409），重复指定同一分区幂等成功。
- **分区继承**：后续导入的新图若关联到已分组，自动继承该组分区。
- **冲突拒绝**：一次导入若把两个不同分区的组连通，整批拒绝（409）并说明冲突涉及哪些组。
- **幂等导入**：同图片 ID 同内容重传不新增；同 ID 异内容整批拒绝（409）。
- **整批原子**：任何一行字段为空（400）或任何冲突，整批不落库。

## 一致性设计

- **连通分析在写事务外完成**：分析阶段只持有只读快照（SQLite WAL 模式，读写互不阻塞），
  期间其他导入和指定分区操作可正常提交。
- **提交时核对最新已确认状态**：进入短写事务后先比对全局状态版本号 `state_version`，
  若分析期间出现了新关联或分区变更，则放弃本次计划、基于最新状态重新分析（有限次重试），
  保证不漏掉新关联、不覆盖分区。
- **失败/重启安全**：所有变更在单个写事务（`BEGIN IMMEDIATE`）中原子提交，失败即整体回滚；
  数据库文件持久化在卷中，进程重启后图片关系与分区保持一致。
- **下载一致性**：每次 CSV 下载在单个只读快照事务中读取，拿到的是完整一致状态
  （响应头 `X-State-Version` 标识该快照对应的版本）。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/imports` | 导入 CSV（multipart 文件上传），整批原子生效 |
| POST | `/partitions/assign` | 按任一图片 ID 将整组指定到 `train`/`val` |
| GET | `/images/unpartitioned` | 未分区图片列表（JSON） |
| GET | `/downloads/{partition}` | 下载分区图片清单 CSV（`train`/`val`/`unpartitioned`） |
| GET | `/images/{image_id}` | 查询单张图片及其组、分区 |
| GET | `/groups` | 全部连通组及分区 |
| GET | `/stats` | 总体统计与状态版本号 |

### CSV 格式

UTF-8（支持带 BOM），必需三列，列名支持中英文：

| 字段 | 可用列名 |
| --- | --- |
| 图片ID | `image_id`、`图片ID` |
| 工件号 | `workpiece_no`、`工件号` |
| 拍摄批次 | `batch_no`、`拍摄批次` |

## 演示流程（sample_data/）

在 Swagger UI 的 `POST /imports` 中点 *Try it out* 上传文件，或用 curl：

```bash
# 1. 导入首批：IMG001~IMG004 沿关联链成一组，IMG005 独立成组
curl -F "file=@sample_data/batch1.csv" http://localhost:8000/imports

# 2. 同内容重传：全部幂等跳过，inserted=0
curl -F "file=@sample_data/batch1.csv" http://localhost:8000/imports

# 3. 异内容重传：409 content_conflict，整批拒绝
curl -F "file=@sample_data/batch5_changed_content.csv" http://localhost:8000/imports

# 4. 按任一图片 ID 把整组（4 张）指定到 train
curl -X POST http://localhost:8000/partitions/assign \
     -H "Content-Type: application/json" -d '{"image_id":"IMG003","partition":"train"}'

# 5. 再导入：IMG006 经批次 LOT-2 关联 train 组 -> 自动继承 train；IMG007 新组未分区
curl -F "file=@sample_data/batch2.csv" http://localhost:8000/imports

# 6. IMG005 组指定为 val
curl -X POST http://localhost:8000/partitions/assign \
     -H "Content-Type: application/json" -d '{"image_id":"IMG005","partition":"val"}'

# 7. IMG008 连接 val 组与未分区组 -> 合并，整组继承 val
curl -F "file=@sample_data/batch3_link.csv" http://localhost:8000/imports

# 8. IMG009 同时关联 val(WP-C) 与 train(LOT-2) -> 409 partition_conflict，整批拒绝并说明冲突
curl -F "file=@sample_data/batch4_conflict.csv" http://localhost:8000/imports

# 9. 下载分区清单，供训练流水线选图
curl -OJ http://localhost:8000/downloads/train
curl -OJ http://localhost:8000/downloads/val
curl http://localhost:8000/images/unpartitioned
```

## 本地开发与测试

```bash
pip install -r requirements.txt
pip install httpx pytest
python3 -m pytest tests/            # 单元/集成测试（分组、幂等、冲突、并发、回滚、重启）

# 启动本地服务并对跑端到端冒烟（含 HTTP 级并发验证）
uvicorn app.main:app &
python3 scripts/smoke_test.py http://127.0.0.1:8000 sample_data/
```

## 目录结构

```
app/
  main.py        # FastAPI 路由、CSV 解析校验
  core.py        # 连通分析（只读快照）+ 版本核对提交、指定分区
  db.py          # SQLite 连接、WAL、事务、schema
sample_data/     # 演示用 CSV
scripts/smoke_test.py
tests/test_api.py
Dockerfile / docker-compose.yml / requirements.txt
```
