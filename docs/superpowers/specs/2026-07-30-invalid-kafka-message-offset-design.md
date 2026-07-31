# 无效 Kafka 消息 Offset 处理设计

## 目标

受控 Worker 遇到 Kafka `value=null`，或者消息缺少教师/学生视频必填字段时，不再退出并反复消费同一个 offset。

## 行为

- 在受控消费者解析消息的位置捕获 `InvalidTaskMessage`。
- 记录 worker、topic、partition、offset、原因和原始 value。
- 不调用业务任务处理器。
- 将失败计数加一并保存最近一次错误。
- 只提交异常消息所在 partition 的 `offset + 1`，不提交其他 partition 的进度。
- 提交后保持 Worker 运行，下一轮继续消费后续消息。
- 不增加死信 Topic。

## 边界

- `teacher_video_path` 和 `student_video_path` 仍然缺一不可；本次不支持单路降级分析。
- 正常任务原有的重试和提交逻辑保持不变。
- 非法 JSON 在 Kafka 反序列化阶段失败的情况不在本次范围内。

## 验证

- `value=null` 时 handler 不执行，精确提交下一 offset。
- 缺少必填视频字段时行为相同。
- 跳过异常消息后，下一条正常消息可以继续处理。
- 完整测试套件无回归。
