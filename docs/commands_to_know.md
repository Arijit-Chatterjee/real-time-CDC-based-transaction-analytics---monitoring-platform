Docker Container Health Checks & Troubleshooting Guide
Reusable command reference for Windows PowerShell, Docker Compose, Kafka, PostgreSQL, Debezium, Spark, data pipelines, and Airflow.

Primary environment: StreamGuard on Windows, project folder C:\StreamGuard.
How to use this guide
- Run the PowerShell commands from Windows PowerShell unless a section explicitly says to run them inside a container.
- For project-specific commands, first open PowerShell in the project folder:
Set-Location C:\StreamGuard
- Replace placeholder names such as <container-name>, <service-name>, and <topic-name> with values from your own project.
- A container being Up does not necessarily mean the application inside it is healthy. Use the application-level checks too.
- Most commands in this guide are read-only checks. Commands that start jobs or change state are labelled.
- Do not run docker compose down -v as routine troubleshooting. The -v option can remove named volumes and destroy local database or pipeline data.
1. Fast checklist: is Docker and the application working?
Run these in order when something appears broken.
1.1 Show running containers
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
What it tells you: which containers are running, their images, how long they have been running, and published ports.
1.2 Show all containers, including stopped ones
docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
What it tells you: whether a missing service exited or failed instead of starting.
1.3 Read recent logs
docker logs --tail 100 <container-name>
Follow logs live (press Ctrl+C to stop watching; this does not stop the container):
docker logs --follow --tail 100 <container-name>
What to look for: ERROR, Exception, Traceback, failed connections, permission errors, out-of-memory messages, and repeated restart messages.
1.4 Check restart count and container state
docker inspect --format 'Name={{.Name}} Status={{.State.Status}} Running={{.State.Running}} RestartCount={{.RestartCount}} Error={{.State.Error}}' <container-name>
What it tells you: whether the container is running, restarting unexpectedly, or has an exit error.
1.5 Check CPU and memory usage
docker stats --no-stream
What it tells you: current container CPU, memory, and network/disk I/O activity. Use it when Spark, Kafka, or another service is slow or crashing.
1.6 Check Docker Engine access
docker version
docker info
What it tells you: whether PowerShell can reach the Docker Engine and whether Docker reports a problem.
2. Docker Compose checks (reusable across projects)
Run these in the folder containing compose.yaml, compose.yml, docker-compose.yaml, or docker-compose.yml.
2.1 List Compose services and status
docker compose ps
2.2 See logs for every service
docker compose logs --tail 100
Follow logs for one service:
docker compose logs --follow --tail 100 <service-name>
2.3 Validate Compose configuration
docker compose config --quiet
No output normally means the Compose file passed validation. An error should identify invalid YAML, settings, or interpolation.
2.4 Start services
docker compose up -d
This starts services in the background. It does not prove every application is ready; perform the relevant health checks afterward.
2.5 View service/container names
docker compose ps --all
Important: Compose service names and Docker container names can differ. docker compose exec takes a service name; docker exec takes a container name.
3. Enter a container and run a command
3.1 Open an interactive shell
For a Linux container with Bash:
docker exec -it <container-name> bash
If Bash is not installed, try:
docker exec -it <container-name> sh
Type exit to leave the shell. This does not stop the container.
3.2 Run one command without opening a shell
docker exec <container-name> <program> <argument1> <argument2>
Example:
docker exec streamguard-postgres pg_isready -U streamguard -d streamguard
3.3 Run a Linux shell command from PowerShell
docker exec <container-name> bash -lc "command here"
Use bash -lc for Linux shell features such as pipes, environment variables, grep, and tail. Be careful with quoting when a command contains both PowerShell and Bash variables.
4. StreamGuard: known container names and ports
Use this to identify the relevant service before running checks. Confirm actual names with docker ps; names can change if the Compose project is recreated.
Component	Container (current known name)	Local port / access
PostgreSQL	streamguard-postgres	Host port 5433
Kafka	streamguard-kafka	Host port 9092
Debezium / Kafka Connect	streamguard-debezium	http://localhost:8083
Debezium UI	streamguard-debezium-ui	http://localhost:8081
Kafka UI	Confirm with docker ps	Commonly http://localhost:8080 in this setup
Spark	streamguard-spark	Spark UI commonly http://localhost:4040 while a Spark job is active
Airflow	streamguard-airflow	http://localhost:8082


If a UI does not open, first confirm the container is running and that the port is actually published by docker ps.
5. PostgreSQL health and data checks
5.1 Check whether PostgreSQL is accepting connections
docker exec streamguard-postgres pg_isready -U streamguard -d streamguard
Healthy result: a message similar to accepting connections.
5.2 Run a SQL query
docker exec streamguard-postgres psql -U streamguard -d streamguard -c "SELECT current_database(), current_user, NOW();"
This checks that a SQL session can be opened. Use the database role configured for your project; do not assume the role is named postgres.
5.3 Check table row counts
docker exec streamguard-postgres psql -U streamguard -d streamguard -c "SELECT 'customers' AS table_name, COUNT(*) FROM public.customers UNION ALL SELECT 'merchants', COUNT(*) FROM public.merchants UNION ALL SELECT 'transactions', COUNT(*) FROM public.transactions;"
Counts should be interpreted against the current stage of your pipeline and data generation.
5.4 Verify the StreamGuard test transaction
docker exec streamguard-postgres psql -U streamguard -d streamguard -c "SELECT transaction_id, amount, currency, region, status, updated_at FROM public.transactions WHERE transaction_id = 'a71c9f42-6e3b-4d85-9a20-1f6b8c3e5d70';"
If this returns a row, the test transaction exists in PostgreSQL. It does not by itself prove CDC or downstream processing works.
5.5 List tables
docker exec streamguard-postgres psql -U streamguard -d streamguard -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"
6. Debezium / Kafka Connect checks
These commands run in Windows PowerShell and call the Connect REST API exposed on port 8083.
6.1 Check connector status
Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors/streamguard-postgres-connector/status" | ConvertTo-Json -Depth 8
Healthy sign: connector state and each task state show RUNNING. A running connector is necessary but does not alone prove the desired table is being captured; check Kafka too.
6.2 List connectors
Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors" | ConvertTo-Json
6.3 Read connector configuration
Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors/streamguard-postgres-connector/config" | ConvertTo-Json -Depth 8
Review table include lists, topic prefix, PostgreSQL connection settings, and slot/publication names. Avoid sharing credentials if your configuration contains secrets.
6.4 Check the Debezium service logs
docker logs --tail 100 streamguard-debezium
7. Kafka health and topic checks
The following Kafka CLI paths match the current apache/kafka:4.1.1 container layout. If a future image uses another layout, inspect the image documentation or locate the CLI scripts first.
7.1 List topics
docker exec streamguard-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
In the current project, the transaction CDC topic is:
streamguard.public.transactions
7.2 Describe the transactions topic
docker exec streamguard-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic streamguard.public.transactions
This checks that the topic exists and displays its partition count and leader/replica information.
7.3 Read a few messages (diagnostic)
docker exec -it streamguard-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streamguard.public.transactions --from-beginning --max-messages 3
This reads the first few messages available from the beginning, which may be snapshot records rather than the newest event. Press Ctrl+C if you need to stop the consumer early. Do not treat this command alone as a check for the latest transaction.
7.4 Kafka service logs
docker logs --tail 100 streamguard-kafka
8. Spark and streaming-job checks
A Spark container can be Up while no Spark application is running. Check for the actual writer processes and their logs.
8.1 Check whether Bronze and Silver writers are running
docker exec streamguard-spark bash -lc "ps -eo pid,etime,args | grep -E '[b]ronze_writer.py|[s]ilver_writer.py|[s]park-submit'"
If nothing relevant is printed, those writer processes may not be running. Verify the logs too before starting a new copy.
8.2 Check Bronze transaction writer log
docker exec streamguard-spark bash -lc "tail -60 /tmp/bronze_transactions.log"
Expected startup messages include the topic, output path, and checkpoint. Continue checking for repeated exceptions after the startup messages.
8.3 Check Silver writer log
docker exec streamguard-spark bash -lc "tail -60 /tmp/silver_writer.log"
The log file may not exist until the writer has been launched.
8.4 Check Spark UI port publication
docker ps --filter "name=streamguard-spark" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
The Spark UI on port 4040 typically exists only while a Spark application is active and the port is published.
8.5 Check lake data directory sizes
docker exec streamguard-spark bash -lc "du -sh /opt/streamguard/lake/bronze /opt/streamguard/lake/silver /opt/streamguard/lake/checkpoints 2>/dev/null"
This reports storage usage, not row counts or data correctness.
8.6 Check a streaming query's log for failures
docker exec streamguard-spark bash -lc "grep -iE 'ERROR|Exception|Traceback|failed|denied|out of memory' /tmp/bronze_transactions.log /tmp/silver_writer.log 2>/dev/null | tail -80"
This is a quick filter, not a full replacement for reading the surrounding log context.
9. StreamGuard: start the Bronze transaction stream
State-changing command — starts a background streaming process. Before running it, use Section 8.1 to check that the same writer is not already running; duplicate writers can compete for the same checkpoint.

Run in PowerShell:
docker exec -d streamguard-spark bash -lc 'mkdir -p /tmp/streamguard-home/.ivy2.5.2; export HOME=/tmp/streamguard-home; export PYTHONPATH="$(printf "%s:" /opt/spark/python/lib/*.zip)"; export STREAMGUARD_KAFKA_BOOTSTRAP=kafka:29092; export STREAMGUARD_KAFKA_TOPIC=streamguard.public.transactions; /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/streamguard-home/.ivy2.5.2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/streamguard/src/streaming/bronze_writer.py > /tmp/bronze_transactions.log 2>&1'
Then check:
docker exec streamguard-spark bash -lc "tail -40 /tmp/bronze_transactions.log"
Expected startup messages:
Bronze streaming started for topic: streamguard.public.transactions
Bronze output: /opt/streamguard/lake/bronze/transactions
Checkpoint: /opt/streamguard/lake/checkpoints/bronze_transactions
These messages mean the query reached its startup code. Keep checking for errors after startup; use the process check to verify it remains alive.
10. StreamGuard: start the Silver writer
State-changing command — starts a continuous Delta Lake writer. Check Section 8.1 first. Do not run multiple copies against the same Silver checkpoints.

Run in PowerShell:
docker exec -d streamguard-spark bash -lc 'cd /opt/streamguard; mkdir -p /tmp/streamguard-ivy; export HOME=/tmp/streamguard-home; export PYTHONPATH="/opt/streamguard:$(printf "%s:" /opt/spark/python/lib/*.zip)"; /opt/spark/bin/spark-submit --master "local[2]" --packages "io.delta:delta-spark_4.2_2.13:4.4.1" --conf spark.jars.ivy=/tmp/streamguard-ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/streamguard/src/silver/silver_writer.py > /tmp/silver_writer.log 2>&1'
Then check:
docker exec streamguard-spark bash -lc "tail -40 /tmp/silver_writer.log"
This command uses the Delta package/version and Spark settings from the project's working Gold pipeline configuration. If dependency resolution fails, inspect the first error in the log before rerunning; repeated package downloads are not a good troubleshooting strategy.
11. Check Silver data quality and Gold pipeline
11.1 Run the project's Silver validation script
This is an application-level validation, not just a container check. Run in PowerShell from C:\StreamGuard:
docker exec streamguard-spark bash -lc 'cd /opt/streamguard; export HOME=/tmp/streamguard-home; export PYTHONPATH="/opt/streamguard:$(printf "%s:" /opt/spark/python/lib/*.zip)"; /opt/spark/bin/spark-submit --master "local[2]" --packages "io.delta:delta-spark_4.2_2.13:4.4.1" --conf spark.jars.ivy=/tmp/streamguard-ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/streamguard/src/silver/validate_silver.py'
Use the script's reported checks and exit status to decide whether Silver is healthy. A validator may fail if the streaming writer is still processing a new event; inspect its output rather than immediately rebuilding tables.
11.2 Run the existing Gold pipeline script
Run in PowerShell from C:\StreamGuard:
.\scripts\run_gold_pipeline.ps1
This performs the project's configured validation/build steps. It is not the same as a continuously running Gold stream; the current Gold builder rebuilds/overwrites Gold tables from the Silver snapshot.
11.3 Check Airflow container and recent logs
docker ps --filter "name=streamguard-airflow" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker logs --tail 100 streamguard-airflow
Airflow UI: http://localhost:8082
11.4 List Airflow DAGs
docker exec streamguard-airflow airflow dags list
List the tasks in the Gold DAG:
docker exec streamguard-airflow airflow tasks list streamguard_gold_pipeline
If the command fails due to a different Airflow CLI version, use the UI and container logs to check the DAG instead.
12. Network and port checks
12.1 List Docker networks
docker network ls
12.2 Inspect the StreamGuard network
docker network inspect streamguard_default
If the network is missing or its name differs, list networks first and inspect the actual project network.
12.3 Show published ports for a container
docker port <container-name>
12.4 Check whether a host TCP port is listening (Windows)
Example for PostgreSQL's host port 5433:
Test-NetConnection -ComputerName localhost -Port 5433
For the API/UI ports, change the port number to 8083, 8082, 9092, or whichever port you are checking.

Interpretation: a listening port proves a TCP endpoint is reachable, not that the application data or pipeline is correct.
13. Storage and resource checks
13.1 Docker's current resource usage
docker stats --no-stream
13.2 Check free space on the Windows C: drive
Get-PSDrive -Name C
13.3 Check filesystem space inside the Spark container
docker exec streamguard-spark bash -lc "df -h / /tmp"
13.4 Check storage used by the Docker daemon
docker system df
This reports Docker image/container/build-cache usage. It does not directly show every host directory mounted into a container.

Avoid cleanup commands such as docker system prune --volumes unless you fully understand what will be removed.
14. Safe restart and stop practices
14.1 Restart one container
docker restart <container-name>
Restarting a database, broker, or streaming container interrupts work. Use only after checking logs and considering dependencies; then rerun the service-specific health check.
14.2 Stop one container
docker stop <container-name>
This stops the entire container and all processes inside it. For example, stopping the Spark container stops all Spark jobs inside it, not only one writer.
14.3 Avoid duplicate streaming writers
Before starting a writer, check running processes and logs:
docker exec streamguard-spark bash -lc "ps -eo pid,etime,args | grep -E '[b]ronze_writer.py|[s]ilver_writer.py'"
Two jobs should not write to the same checkpoint path at the same time. If a writer must be stopped, identify the correct process PID first and stop only that process; do not blindly kill all Java processes because other Spark jobs may be running.
14.4 Protect persisted data
- Do not delete a streaming checkpoint as a first troubleshooting step. It can cause rereading or skipping behavior depending on source and options.
- Do not delete Bronze, Silver, Gold, or PostgreSQL data directories without an explicit recovery plan.
- Do not use docker compose down -v unless you intend to remove Compose-managed volumes.
- Prefer inspecting logs, process state, checkpoints, and validation output before resetting anything.
15. A reusable troubleshooting decision tree
When a new event does not appear downstream, check each boundary in order:
1. Source database: does the row exist in PostgreSQL?
2. CDC connector: does the Debezium connector and its task show RUNNING?
3. Kafka: does the expected topic exist, and can a consumer read events?
4. Bronze: is the Bronze writer process alive, and is its log free from new errors?
5. Silver: is the Silver writer alive, and does its validation pass?
6. Gold: has the Gold build/refresh run after Silver processed the event?
7. Orchestration: if Airflow is responsible for triggering a job, did the DAG run and did each task succeed?
Key principle: test one boundary at a time. If PostgreSQL and Kafka contain the event but Bronze does not, investigate Bronze before changing Debezium or rebuilding Gold.
Quick health report commands
These are useful to paste first when asking for help:
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker stats --no-stream
docker logs --tail 40 streamguard-debezium
docker logs --tail 40 streamguard-kafka
docker exec streamguard-spark bash -lc "ps -eo pid,etime,args | grep -E '[b]ronze_writer.py|[s]ilver_writer.py|[s]park-submit'"
docker exec streamguard-spark bash -lc "tail -30 /tmp/bronze_transactions.log; echo '--- SILVER ---'; tail -30 /tmp/silver_writer.log"
Before sharing output publicly, scan it for credentials, tokens, private paths, or sensitive records.
16. Future-project template
For a new Docker-based project, fill in this small service map before troubleshooting:
Item	Your project's value
Project folder	...
Compose file location	...
Compose service name	...
Container name	...
Health endpoint or readiness command	...
Log command	docker logs --tail 100 <container-name>
Important ports	...
Persistent data / volume paths	...
Validation command	...

## Start missing streaming writers

From the project root in PowerShell:

```powershell
.\scripts\start_streaming_writers.ps1

This prevents assumptions about names, ports, database users, or container image layouts when moving from StreamGuard to a different project or cloud environment.
$commands = @'
## Reliability verification commands

### Run automated tests and code checks

Run from the project root in PowerShell:

```powershell
uv run pytest -q
uv run ruff check src/gold/gold_incremental_writer.py src/silver/silver_writer.py
git diff --check


## Reliability verification commands

### Run automated tests and code checks

From the project root in PowerShell:

```powershell
uv run pytest -q
uv run ruff check src/gold/gold_incremental_writer.py src/silver/silver_writer.py
git diff --check
```

These commands run the unit tests, lint the selected files, and check for whitespace errors.

### Check active Gold writer processes

```powershell
docker exec streamguard-spark bash -lc 'ps -eo pid,ppid,etime,args | grep -E "[g]old_incremental_writer.py"'
```

Identify the Python driver process before restarting Gold. The output can include its parent shell and Java Spark process, so do not count every line as a separate writer.

### Inspect Gold micro-batch results

Find recent Gold log files:

```powershell
docker exec streamguard-spark bash -lc 'ls -lt /tmp/gold_incremental_writer*.log | head'
```

Inspect the log belonging to the current writer:

```powershell
docker exec streamguard-spark bash -lc 'grep -E "transactions batch .*rows=|insert-only append path|completed in Gold|ERROR|Exception|Traceback" /tmp/gold_incremental_writer_insertappend.log | tail -n 25'
```

Replace the example filename when a different log is active.

### Check transaction-ID uniqueness in Gold

Run from `C:\StreamGuard`:

```powershell
uv run --extra dashboard python -c "from deltalake import DeltaTable; df = DeltaTable('lake/gold/fact_transactions').to_pandas(columns=['transaction_id']); ids = df['transaction_id']; print(f'Total rows: {len(ids)}'); print(f'Distinct IDs: {ids.nunique()}'); print(f'Duplicate-key rows: {len(ids) - ids.nunique()}')"
```

A zero duplicate count verifies the current Gold snapshot, not every possible future retry scenario.

### Recovery-test safety

- Do not delete streaming checkpoints during a normal restart test.
- Do not start a second writer against an active writer's checkpoint.
- For a normal restart test, stop Gold cleanly, verify Bronze and Silver remain active, then restart Gold using the same checkpoint.
- Do not deliberately crash the live writer at a commit boundary without a separate test environment and a defined recovery procedure.
