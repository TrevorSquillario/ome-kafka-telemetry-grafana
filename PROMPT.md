I’m looking ingests kafka events from Dell’s OpenManage Enterprise (OME). Add the telemetry to TimescaleDB and visualize the metrics in Grafana. Read the documentation links below. Build me the application in python using a services folder to application services, include a Dockerfile and compose.yaml. Application configuration variables should be stored in a .env file and added to the containers environment. 

- Utilize the existing stream_processor.py
- Add a timescaledb service to add points to a database. Create necessary docker compose service
- Add necessary methods to ome_helper.py to import the telemetry metrics based on examples/telemetry.json. Keep the ome specific logic here. Keep the timescaledb service generalized
- Create necessary grafana docker compose service and ensure it can connect to the timescaledb datasource


OME Documentation
https://www.dell.com/support/manuals/en-us/dell-openmanage-enterprise/ome_4_6_online_help_user_guide/export-data-from-openmanage-enterprise-to-kafka?guid=guid-cf0f5ff8-e595-4a0f-be12-1c884bcd6a77&lang=en-us
https://www.dell.com/support/manuals/en-us/dell-openmanage-enterprise/ome_4_6_online_help_user_guide/enable-kafka-connectivity-in-openmanage-enterprise?guid=guid-cc26aa76-d8eb-4138-a073-3bbf65d8522c&lang=en-us