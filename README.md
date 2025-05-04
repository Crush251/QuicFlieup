# QUIC文件上传系统

基于QUIC协议的高性能文件上传系统，支持大文件分片上传、断点续传、秒传等功能。系统集成了用户认证、Redis缓存、RabbitMQ消息队列和MySQL数据库持久化存储。

## 功能特性

- **QUIC协议**: 利用QUIC协议的多路复用和零RTT特性，提供更快的文件上传体验
- **用户管理**: 支持用户注册、登录和用户信息管理
- **文件管理**: 支持文件元数据存储和查询
- **断点续传**: 记录上传进度，支持断点续传功能
- **秒传功能**: 基于文件哈希判断，实现文件秒传
- **高性能处理**: 使用Redis管理上传状态，支持高并发上传
- **异步处理**: 使用RabbitMQ处理文件合并等耗时操作
- **持久化存储**: 使用MySQL存储用户和文件元数据信息

## 系统架构

系统主要由以下几个部分组成：

1. **用户服务**: 处理用户注册、登录和信息获取
2. **文件上传服务**: 处理文件初始化、分片上传和完成上传
3. **文件合并服务**: 从消息队列获取合并任务，执行文件合并
4. **数据层**: MySQL数据库存储用户和文件信息
5. **缓存层**: Redis管理上传进度和文件元数据
6. **消息队列**: RabbitMQ处理异步任务

## 系统要求

- Go 1.20或更高版本
- MySQL 8.0或更高版本
- Redis 6.0或更高版本
- RabbitMQ 3.8或更高版本

## 快速开始

### 安装依赖

确保已经安装了所需的Go、MySQL、Redis和RabbitMQ服务。

### 创建数据库

使用`server/database/schema.sql`创建必要的数据库表：

```bash
mysql -u root -p < server/database/schema.sql
```

### 配置服务

修改`start.sh`中的参数，或者在启动时通过命令行参数指定：

```bash
./start.sh --mysql-user=myuser --mysql-pass=mypass --redis-host=myredis --mq-host=mymq
```

### 运行服务

```bash
./start.sh
```

## API接口

### 用户相关

- `POST /api/user/register` - 用户注册
- `POST /api/user/login` - 用户登录
- `GET /api/user/info` - 获取用户信息（需要认证）
- `POST /api/user/logout` - 用户登出

### 文件上传相关

- `POST /api/initUpload` - 初始化文件上传
- `GET /api/getUploadedChunks` - 获取已上传的分片列表
- `POST /api/uploadChunk` - 上传文件分片
- `POST /api/completeUpload` - 完成文件上传
- `GET /api/userFiles` - 获取用户文件列表（需要认证）

## 开发者

如果您想要贡献代码或自定义系统，可以参考以下步骤：

1. Fork本仓库
2. 创建您的功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 打开一个Pull Request

## 许可证

MIT