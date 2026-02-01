@echo off
echo Downloading SQLite command-line tool...
curl -L https://www.sqlite.org/2024/sqlite-tools-win-x64-3470200.zip -o sqlite-tools.zip

echo Extracting...
tar -xf sqlite-tools.zip

echo Running SQL script...
sqlite-tools-win-x64-3470200\sqlite3.exe orders.db < reset-skipped-deliveries.sql

echo Done! Skipped delivery emails have been reset to pending.
pause
