-- 创建员工消息表
CREATE TABLE IF NOT EXISTS employee_messages (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    department VARCHAR(255),
    salary DOUBLE,
    message_time TIMESTAMP(3),
    process_time TIMESTAMP(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);