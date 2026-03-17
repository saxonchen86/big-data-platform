# .devcontainer/Dockerfile
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# 1. 安装基础工具和 JDK 17 (系统级)
RUN apt-get update && apt-get install -y \
    curl zip unzip git zsh build-essential \
    software-properties-common vim net-tools iputils-ping \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# 2. 安装 uv (极速 Python 包管理)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 3. 安装 SDKMAN 并配置 JDK 17 + Scala 2.13
# 这里显式安装 17.0.10-tem，这是最匹配 Flink 1.18 的版本
ENV SDKMAN_DIR="/root/.sdkman"
RUN curl -s "https://get.sdkman.io" | bash \
    && bash -c "source $HOME/.sdkman/bin/sdkman-init.sh && \
    sdk install java 17.0.10-tem && \
    sdk install scala 2.13.12 && \
    sdk install sbt 1.9.8 && \
    sdk install maven 3.9.6"

# 4. 环境变量配置
ENV PATH="$SDKMAN_DIR/candidates/java/current/bin:$SDKMAN_DIR/candidates/scala/current/bin:$SDKMAN_DIR/candidates/sbt/current/bin:$SDKMAN_DIR/candidates/maven/current/bin:$PATH"
ENV JAVA_HOME="$SDKMAN_DIR/candidates/java/current"

# 5. 安装 Python 3.13 和大数据组件
RUN uv python install 3.13
ENV VIRTUAL_ENV=/opt/venv
RUN uv venv $VIRTUAL_ENV --python 3.13
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# 安装常用大数据库
RUN uv pip install \
    pyspark==3.5.0 

WORKDIR /workspace
CMD ["sleep", "infinity"]
