@echo off
REM Create all required Kafka topics for the GDELT pipeline
REM Usage: create-topics.bat [bootstrap-server]
REM Default bootstrap server: localhost:9092

SET BOOTSTRAP=%1
IF "%BOOTSTRAP%"=="" SET BOOTSTRAP=localhost:9092

SET KAFKA_BIN=%KAFKA_HOME%\bin\windows
IF "%KAFKA_HOME%"=="" SET KAFKA_BIN=C:\kafka\bin\windows

echo Creating Kafka topics on %BOOTSTRAP%...

FOR %%T IN (gdelt.events.raw gdelt.mentions.raw gdelt.gkg.raw gdelt.events.normalized gdelt.analytics gdelt.dead-letter) DO (
  echo   Creating topic: %%T
  call "%KAFKA_BIN%\kafka-topics.bat" --create --bootstrap-server %BOOTSTRAP% --topic %%T --partitions 3 --replication-factor 1 --if-not-exists --config retention.ms=604800000
)

echo.
echo Listing all topics:
call "%KAFKA_BIN%\kafka-topics.bat" --list --bootstrap-server %BOOTSTRAP%
echo.
echo Done.

