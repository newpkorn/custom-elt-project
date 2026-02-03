import subprocess
import time
import sys

def wait_for_postgres(host, max_retries=30, delay_seconds=2):
    for attempt in range(1, max_retries + 1):
        result = subprocess.run(
            ["pg_isready", "-h", host],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print(f"PostgreSQL is ready on host {host}")
            return True

        print(f"[{host}] not ready ({attempt}/{max_retries})")
        time.sleep(delay_seconds)

    return False


for host in ["source_postgres", "destination_postgres"]:
    if not wait_for_postgres(host):
        print(f"PostgreSQL {host} not ready, exiting")
        sys.exit(1)

print("Starting ELT process...")

source_config = {
    "dbname": "source_db",
    "user": "postgres",
    "password": "secret",
    "host": "source_postgres",
}

destination_config = {
    "dbname": "destination_db",
    "user": "postgres",
    "password": "secret",
    "host": "destination_postgres",
}

dump_command = [
    "pg_dump",
    "-h", source_config["host"],
    "-U", source_config["user"],
    "-d", source_config["dbname"],
    "-f", "data_dump.sql"
]

subprocess.run(
    dump_command,
    env={"PGPASSWORD": source_config["password"]},
    check=True
)

load_command = [
    "psql",
    "-h", destination_config["host"],
    "-U", destination_config["user"],
    "-d", destination_config["dbname"],
    "-f", "data_dump.sql"
]

subprocess.run(
    load_command,
    env={"PGPASSWORD": destination_config["password"]},
    check=True
)

print("ELT process completed successfully.")
