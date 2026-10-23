# YugabyteDB 2025.2.0.0, macOS arm64 build. Port 7000 is taken by macOS (AirPlay), hence 17000.
./bin/yugabyted start --base_dir <dir> --advertise_address 127.0.0.1 --master_webserver_port 17000 \
  --tserver_flags "ysql_max_connections=1100,durable_wal_write=true" --background true
