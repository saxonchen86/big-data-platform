#!/bin/bash

echo "Building Flink Employee Message Processor..."

# 编译项目
mvn clean package -DskipTests

if [ $? -eq 0 ]; then
    echo "Build successful!"
    echo "To run the job on Flink cluster:"
    echo "1. Copy the jar file to Flink's lib directory"
    echo "2. Submit the job using:"
    echo "   bin/flink run -c com.example.flink.EmployeeMessageProcessor target/flink-kafka-employee-processor-1.0-SNAPSHOT.jar"
else
    echo "Build failed!"
    exit 1
fi