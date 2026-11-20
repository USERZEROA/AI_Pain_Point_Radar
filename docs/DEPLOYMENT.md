# Public deployment

Run the full stack on a Linux server with Python 3, Docker and Compose v2. Point a public DNS record to the server and allow inbound ports 80 and 443.

```bash
git clone https://github.com/USERZEROA/AI_Pain_Point_Radar.git
cd AI_Pain_Point_Radar
python3 scripts/start.py --public-domain YOUR_HOSTNAME
```

The launcher creates a local `.env` with a random PostgreSQL password. Caddy obtains an HTTPS certificate and forwards HTTP and WebSocket traffic to Streamlit. Kafka and PostgreSQL use the Compose network. Only the gateway has public port bindings.

Check the health endpoint from outside the host. Open the dashboard in a private browser window and watch the analyzed count change while replay is running.

```bash
curl --fail https://YOUR_HOSTNAME/_stcore/health
docker compose ps
docker compose logs --tail 30 producer
```

After updating the checkout, run the same launch command to rebuild. To stop the deployment while retaining the data volumes, use `docker compose -f docker-compose.yml -f docker-compose.public.yml down`.

## Google Cloud demo

The demo runs on a Compute Engine VM with a reserved public IPv4 address. The VM startup script mounts a Caddy configuration that requests and renews a Let's Encrypt certificate for the IP address. To check the running services from the VM, include its Compose override.

```bash
cd /opt/ai-pain-point-radar
sudo docker compose -f docker-compose.yml -f docker-compose.public.yml \
  -f /etc/ai-pain-point-radar/compose.yml ps
```
