-- 开启 Tableau 模式查看结果
SET 'sql-client.execution.result-mode' = 'tableau';

-- 1. 定义 JDBC Catalog (用于连接 MySQL 维表)
-- CREATE CATALOG mysql_catalog WITH (
--     'type' = 'jdbc',
--     'default-database' = 'flink_dev',
--     'username' = 'root',
--     'password' = 'streampark',
--     'base-url' = 'jdbc:mysql://streampark-db:3306'
-- );

-- 2. 定义 Kafka/Datagen 表 (必须放在 default_catalog 内存里)
-- 注意：这里不要 USE mysql_catalog，否则下面会报错
-- USE CATALOG default_catalog;

CREATE TABLE IF NOT EXISTS datagen (
    id INT,
    name STRING
) WITH (
    'connector' = 'datagen'
);

-- 3. (可选) 切回 mysql_catalog 方便查询维表，或者保持在 default
-- USE CATALOG mysql_catalog;



USE streampark;
SELECT job_name FROM t_flink_app;


SELECT 1