from vision_orchestrator.config import VisionOrchestratorConfig


def create_mysql_connection(config: VisionOrchestratorConfig):
    try:
        import pymysql
        from pymysql.cursors import DictCursor
    except ModuleNotFoundError as exc:
        raise RuntimeError("缺少 PyMySQL 依赖，请安装 vision_orchestrator/requirements.txt") from exc

    return pymysql.connect(
        host=config.db_host,
        port=config.db_port,
        user=config.db_user,
        password=config.db_password,
        database=config.db_name,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )
