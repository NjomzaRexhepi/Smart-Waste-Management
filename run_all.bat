@echo off
REM ============================
REM Smart Waste Management Startup Script
REM ============================

REM --- 0. Delete old Kafka logs if present ---
if exist "C:\tmp\kafka-logs2" (
    echo Deleting old Kafka logs at C:\kafka\logs ...
    rmdir /s /q "C:\tmp\kafka-logs2"
    echo Old logs deleted.
)

REM --- 1. Start Zookeeper ---
start "Zookeeper" cmd /k "C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties"

REM Wait a few seconds to ensure Zookeeper starts
timeout /t 5 /nobreak >nul

REM --- 2. Start Kafka Broker ---
start "Kafka" cmd /k "C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties"

REM Wait until Kafka port 9092 is open
:WaitKafka
powershell -Command "while (!(Test-NetConnection -ComputerName localhost -Port 9092).TcpTestSucceeded) { Start-Sleep -Seconds 2 }"
echo Kafka is ready!

REM --- 4. Start Simulator ---
start "Simulator" cmd /k "python data_simulator\simulator.py --kafka-brokers localhost:9092 --kafka-topic waste-sensor-data --interval 10"

REM --- 5. Start Spark Streaming ---
start "Spark Streaming" cmd /k "C:\spark\bin\spark-submit.cmd --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 C:\Users\Admin\IoT_Project\Smart-Waste-Management\spark_processing\waste_bin_streaming.py"

echo All processes started!
