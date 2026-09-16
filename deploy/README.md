# Deployment

MacroHarvey runs as a single `uvicorn` process behind Caddy (TLS + reverse
proxy) on the server at `178.104.96.245`, reachable at
`https://178-104-96-245.sslip.io`.

```
Internet ──HTTPS──▶ Caddy (:443, auto TLS via sslip.io)
                      └─reverse_proxy─▶ uvicorn main:app (127.0.0.1:8000)
                                          ├─ FastAPI HTTP API + Mini App
                                          ├─ Telegram long-poll loop
                                          └─ APScheduler (reports, alerts)
```

## First-time server setup

1. **Code** — clone into `/opt/macroharvey`:
   ```bash
   sudo mkdir -p /opt/macroharvey && sudo chown "$USER" /opt/macroharvey
   git clone <your-repo-url> /opt/macroharvey
   cd /opt/macroharvey
   ```
2. **Config** — create `.env` from the template and fill in secrets:
   ```bash
   cp .env.example .env && nano .env
   ```
   Make sure to set a long random `ADMIN_TOKEN` and keep `TG_AUTH_REQUIRED=1`.
3. **Virtualenv + deps**:
   ```bash
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```
4. **Fonts** (for PDF reports) — either install system DejaVu or rely on the
   bundled copy in `assets/fonts/`:
   ```bash
   sudo apt-get install -y fonts-dejavu-core
   ```
5. **systemd service**:
   ```bash
   sudo cp deploy/macroharvey.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now macroharvey
   ```
6. **Caddy**:
   ```bash
   sudo cp deploy/Caddyfile /etc/caddy/Caddyfile
   sudo systemctl reload caddy
   ```

## Autodeploy (GitHub Actions)

`.github/workflows/deploy.yml` SSHes into the server on every push to `main`
and runs `deploy/deploy.sh` (git pull → pip install → restart service).

Add these repository secrets:

| Secret     | Value                                                    |
|------------|---------------------------------------------------------|
| `SSH_HOST` | `178.104.96.245`                                         |
| `SSH_USER` | deploy user (needs sudo for `systemctl restart`)        |
| `SSH_KEY`  | private key; public half in the server `authorized_keys`|
| `SSH_PORT` | optional (default 22)                                    |

Give the deploy user passwordless sudo for just the restart, e.g. in
`/etc/sudoers.d/macroharvey`:
```
deployuser ALL=(root) NOPASSWD: /bin/systemctl restart macroharvey, /bin/systemctl status macroharvey
```

## Manual deploy

```bash
ssh user@178.104.96.245
cd /opt/macroharvey && bash deploy/deploy.sh
```

## Logs

```bash
journalctl -u macroharvey -f      # app logs (now via Python logging)
journalctl -u caddy -f            # proxy / TLS
```
