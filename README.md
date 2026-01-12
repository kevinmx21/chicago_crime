# Chicago Crime Data Pipeline

基于 Databricks Asset Bundles 的芝加哥犯罪数据 AutoLoader Pipeline，支持 DEV/PROD 环境分离和 CI/CD 自动部署。

## 📁 项目结构

```
chicago_crime/
├── databricks.yml              # DAB 主配置文件
├── resources/
│   └── chicago_crime_pipeline.yml  # Pipeline/Job 定义
├── src/
│   └── notebooks/
│       └── ingest_crime_data.py    # AutoLoader Notebook
├── .github/
│   └── workflows/
│       └── deploy.yml              # GitHub Actions CI/CD
└── README.md
```

## 🏗️ 架构

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Azure Storage  │ ──▶ │    AutoLoader    │ ──▶ │   Delta Table   │
│  (Raw CSV/JSON) │     │  (Databricks)    │     │ (Unity Catalog) │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        │                        │                        │
        ▼                        ▼                        ▼
   kevintestdatabricks      Incremental Load       dev_catalog.chicago_crime.crime_data
                                                   prod_catalog.chicago_crime.crime_data
```

## 🚀 快速开始

### 1. 前置条件

- Databricks CLI 已安装
- Azure Storage Account 配置完成
- Unity Catalog 已启用

### 2. 安装 Databricks CLI

```bash
pip install databricks-cli
```

### 3. 配置认证

```bash
# 方式1: 使用 PAT Token
databricks configure --token
# Host: https://adb-7405616484370045.5.azuredatabricks.net
# Token: <your-pat-token>

# 方式2: 使用环境变量
export DATABRICKS_HOST=https://adb-7405616484370045.5.azuredatabricks.net
export DATABRICKS_TOKEN=<your-pat-token>
```

### 4. 本地部署

```bash
# 验证配置
databricks bundle validate -t dev

# 部署到 DEV
databricks bundle deploy -t dev

# 部署到 PROD
databricks bundle deploy -t prod

# 运行 Pipeline
databricks bundle run -t dev chicago_crime_ingest
```

## 🔧 环境配置

| 环境 | Catalog | Schema | Container | 触发分支 |
|------|---------|--------|-----------|----------|
| DEV  | dev_catalog | chicago_crime | dev | `develop` |
| PROD | prod_catalog | chicago_crime | prod | `main` |

## 📦 Unity Catalog 设置

在 Databricks 中执行以下 SQL 创建必要的资源：

```sql
-- DEV 环境
CREATE CATALOG IF NOT EXISTS dev_catalog;
CREATE SCHEMA IF NOT EXISTS dev_catalog.chicago_crime;

-- PROD 环境
CREATE CATALOG IF NOT EXISTS prod_catalog;
CREATE SCHEMA IF NOT EXISTS prod_catalog.chicago_crime;
```

## 🔐 GitHub Secrets 配置

在 GitHub 仓库中配置以下 Secrets：

| Secret 名称 | 说明 |
|------------|------|
| `DATABRICKS_TOKEN` | Databricks Personal Access Token |

### 获取 Token

1. 登录 Databricks Workspace
2. 点击右上角用户图标 → User Settings
3. Access tokens → Generate new token
4. 复制 Token 到 GitHub Secrets

## 🔄 CI/CD 工作流

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  feature/*  │ ──▶ │   develop   │ ──▶ │    main     │
│  (开发)     │     │   (DEV)     │     │   (PROD)    │
└─────────────┘     └─────────────┘     └─────────────┘
                          │                    │
                          ▼                    ▼
                    Auto Deploy           Auto Deploy
                      to DEV              to PROD
```

### 分支策略

- `feature/*`: 功能开发分支
- `develop`: DEV 环境部署分支
- `main`: PROD 环境部署分支

### 部署流程

1. 创建 feature 分支开发
2. 提交 PR 到 `develop`
3. PR 合并后自动部署到 DEV
4. 测试通过后提交 PR 到 `main`
5. PR 合并后自动部署到 PROD

## 📊 Storage 路径配置

在 Azure Storage Account `kevintestdatabricks` 中创建以下目录结构：

```
# DEV 环境
dev/
├── chicago_crime/
│   └── raw/          # 放置原始 CSV 文件
└── _checkpoints/
    └── chicago_crime/
        ├── schema/   # Schema 检查点
        └── data/     # 数据检查点

# PROD 环境
prod/
├── chicago_crime/
│   └── raw/
└── _checkpoints/
    └── chicago_crime/
        ├── schema/
        └── data/
```

## 🛠️ 常用命令

```bash
# 验证 bundle 配置
databricks bundle validate -t dev

# 部署但不运行
databricks bundle deploy -t dev

# 部署并运行
databricks bundle deploy -t dev
databricks bundle run -t dev chicago_crime_ingest

# 查看部署状态
databricks bundle summary -t dev

# 销毁部署的资源
databricks bundle destroy -t dev
```

## 📝 数据字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 唯一标识符 |
| case_number | STRING | 案件编号 |
| crime_date | STRING | 犯罪日期时间 |
| block | STRING | 街区地址 |
| primary_type | STRING | 犯罪类型 |
| description | STRING | 详细描述 |
| location_description | STRING | 地点描述 |
| arrest | BOOLEAN | 是否逮捕 |
| domestic | BOOLEAN | 是否家庭纠纷 |
| beat | INT | 警区编号 |
| district | INT | 区域编号 |
| latitude | DOUBLE | 纬度 |
| longitude | DOUBLE | 经度 |
| _ingestion_timestamp | TIMESTAMP | 数据摄取时间 |
| _source_file | STRING | 源文件路径 |
| _environment | STRING | 环境标识 |

## 🔍 故障排除

### 常见问题

1. **权限错误**
   ```
   Error: PERMISSION_DENIED
   ```
   解决: 确保 Service Principal 或用户有 Storage Account 和 Unity Catalog 的访问权限

2. **Schema 推断失败**
   ```
   Error: Unable to infer schema
   ```
   解决: 检查 CSV 文件格式，确保有 header 行

3. **Checkpoint 冲突**
   ```
   Error: checkpoint location already exists
   ```
   解决: 如果需要重新开始，删除 `_checkpoints` 目录

## 📚 参考资料

- [Databricks Asset Bundles 文档](https://docs.databricks.com/dev-tools/bundles/index.html)
- [AutoLoader 文档](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Unity Catalog 文档](https://docs.databricks.com/data-governance/unity-catalog/index.html)

## 📄 License

MIT License
