# 可选写入 selection_mode 设计

## 目标

同一个 vision_orchestrator 镜像同时兼容包含和不包含 `lesson_snapshot_event.selection_mode` 的数据库版本。

## 配置

新增布尔配置 `WriteSnapshotSelectionMode`：

- 默认 `true`，保持当前写入 `selection_mode=1` 的行为。
- 配置为 `false` 时，快照 INSERT 参数、列、VALUES 和重复键更新均不包含 `selection_mode`。

示例配置必须注明：旧表没有该列时设置为 `false`；数据库已增加该列时保持 `true`。

## 数据流

配置加载器将值解析为 `write_snapshot_selection_mode`，Worker 工厂在创建数据库仓储时传入。仓储只负责根据该布尔值选择对应的快照 SQL，不查询数据库元数据，也不捕获 1054 后隐式降级。

## 边界与错误处理

- 不自动执行数据库 DDL。
- 不在运行时探测表结构，避免额外权限和启动依赖。
- 配置与数据库结构不匹配时保留数据库原始异常，便于发现部署错误。
- 其他表写入和 Kafka offset 行为不变。

## 验证

- 默认配置仍写入 `selection_mode=1`。
- 配置为 `false` 时 SQL 与参数均不包含该字段。
- TOML 显式配置 `false` 能正确传入仓储。
- 完整测试套件无回归。
