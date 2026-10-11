# ============================================================================
# 可选镜像 —— **默认不用**。
#
# 默认直接吃官方镜像 `apache/airflow:2.10.5-python3.12` 就够了：
# fixture 模式下 core 只用到 PyYAML / requests（Airflow 镜像自带），
# `futu` 在 core 里是**懒导入**（只在 `_from_futu` 分支里 `from futu import ...`），
# 所以**不需要 futu-api / pandas**。
#
# 什么时候才用这个文件：
#   - 想切 `source: futu`（要 futu-api，而它硬依赖 pandas），或
#   - 想给容器补任何第三方依赖。
#
# 用法：把 docker-compose.yaml 里的 `image:` 换成
#     build: { context: ., dockerfile: docker/Dockerfile }
# （context 是 `airflow/` 目录，所以下面 COPY 的路径相对 airflow/）
# ============================================================================
FROM apache/airflow:2.10.5-python3.12

USER airflow

# 切真机数据源时要打开这两行（fixture 模式不需要）：
# RUN pip install --no-cache-dir "futu-api>=10.11" "pandas>=2.0"

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
