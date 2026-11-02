#!/usr/bin/env bash
set -euo pipefail

DOMAIN="baw-sam.uz"
EXPECTED_IPV4="77.83.206.140"
APP_DIR="/opt/samsara/app"
STATE_DIR="/var/lib/samsara"
ENV_FILE="/etc/samsara-bot.env"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run this script as root (for example: sudo bash deploy/configure-ubuntu.sh)." >&2
  exit 1
fi
if [[ ! -f "$APP_DIR/main.py" || ! -f "$APP_DIR/miniapp_server.py" || \
      ! -f "$APP_DIR/requirements.txt" || ! -f "$APP_DIR/webapp/index.html" || \
      ! -f "$APP_DIR/deploy/nginx-baw-sam.uz.conf" || ! -f "$APP_DIR/deploy/samsara-bot.service" ]]; then
  echo "Upload main.py, requirements.txt, webapp/, and deploy/ to $APP_DIR first." >&2
  exit 1
fi
if [[ ! -f "$ENV_FILE" && ! -f "$APP_DIR/.env" ]]; then
  echo "Upload the populated .env to $APP_DIR/.env before configuring the service." >&2
  exit 1
fi
if ! getent ahostsv4 "$DOMAIN" | awk '{print $1}' | grep -Fxq "$EXPECTED_IPV4"; then
  echo "$DOMAIN must resolve to $EXPECTED_IPV4 before requesting the HTTPS certificate." >&2
  exit 1
fi

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y \
  ca-certificates ffmpeg fonts-dejavu-core nginx python3 python3-pip python3-venv
if ! command -v certbot >/dev/null 2>&1; then
  DEBIAN_FRONTEND=noninteractive apt-get install -y certbot python3-certbot-nginx
fi

if ! getent group samsara >/dev/null; then
  groupadd --system samsara
fi
if ! id samsara >/dev/null 2>&1; then
  useradd --system --gid samsara --home-dir "$STATE_DIR" --shell /usr/sbin/nologin samsara
fi
install -d -o root -g samsara -m 0750 "$APP_DIR"
install -d -o samsara -g samsara -m 0750 "$STATE_DIR" "$STATE_DIR/pti_uploads"

if [[ ! -f "$ENV_FILE" ]]; then
  install -o root -g samsara -m 0640 "$APP_DIR/.env" "$ENV_FILE"
fi
rm -f "$APP_DIR/.env"
if [[ -f "$APP_DIR/samsara_bot.sqlite3" && ! -f "$STATE_DIR/samsara_bot.sqlite3" ]]; then
  install -o samsara -g samsara -m 0600 "$APP_DIR/samsara_bot.sqlite3" "$STATE_DIR/samsara_bot.sqlite3"
fi
if [[ -d "$APP_DIR/pti_uploads" ]]; then
  cp -an "$APP_DIR/pti_uploads/." "$STATE_DIR/pti_uploads/"
  rm -rf "$APP_DIR/pti_uploads"
fi
rm -f "$APP_DIR/samsara_bot.sqlite3"
python3 - "$ENV_FILE" "$STATE_DIR/samsara_bot.sqlite3" <<'PY'
from pathlib import Path
import sys

env_file = Path(sys.argv[1])
state_db = sys.argv[2]
keys = {
    "WEBAPP_URL": "https://baw-sam.uz/",
    "WEBAPP_HOST": "127.0.0.1",
    "WEBAPP_PORT": "8080",
    "STATE_DB": state_db,
}
lines = env_file.read_text(encoding="utf-8").splitlines()
remaining = [line for line in lines if line.split("=", 1)[0].strip() not in keys]
remaining.extend(f"{key}={value}" for key, value in keys.items())
env_file.write_text("\n".join(remaining) + "\n", encoding="utf-8")
PY
chown root:samsara "$ENV_FILE"
chmod 0640 "$ENV_FILE"

chown -R root:samsara "$APP_DIR"
find "$APP_DIR" -type d -exec chmod 0750 {} +
find "$APP_DIR" -type f -exec chmod 0640 {} +
chown -R samsara:samsara "$STATE_DIR"

python3 -m venv /opt/samsara/venv
"/opt/samsara/venv/bin/pip" install --disable-pip-version-check -r "$APP_DIR/requirements.txt"

NGINX_SITE="/etc/nginx/sites-available/samsara-bot"
DOMAIN_PATTERN='server_name[^;]*([[:space:]]|^)baw-sam[.]uz([[:space:];]|$)'
if [[ -e "$NGINX_SITE" ]] && ! grep -Eq "$DOMAIN_PATTERN" "$NGINX_SITE"; then
  echo "$NGINX_SITE already exists and does not appear to be the baw-sam.uz site; inspect it before continuing." >&2
  exit 1
fi
if [[ -e "$NGINX_SITE" ]] && ! grep -Fq 'proxy_pass http://127.0.0.1:8080;' "$NGINX_SITE"; then
  echo "$NGINX_SITE already serves $DOMAIN through another upstream; inspect it before continuing." >&2
  exit 1
fi
for enabled_site in /etc/nginx/sites-enabled/* /etc/nginx/conf.d/*.conf; do
  [[ -f "$enabled_site" ]] || continue
  [[ "$(readlink -f "$enabled_site")" == "$(readlink -f "$NGINX_SITE" 2>/dev/null || true)" ]] && continue
  if grep -Eq "$DOMAIN_PATTERN" "$enabled_site"; then
    echo "An enabled Nginx site already contains $DOMAIN ($enabled_site). Inspect and reuse it instead of replacing it." >&2
    exit 1
  fi
done
if [[ ! -e "$NGINX_SITE" ]]; then
  install -o root -g root -m 0644 "$APP_DIR/deploy/nginx-baw-sam.uz.conf" "$NGINX_SITE"
  ln -sfn "$NGINX_SITE" /etc/nginx/sites-enabled/samsara-bot
fi
nginx -t
systemctl enable --now nginx

install -o root -g root -m 0644 "$APP_DIR/deploy/samsara-bot.service" /etc/systemd/system/samsara-bot.service
systemctl daemon-reload

echo "Requesting a Let's Encrypt certificate for $DOMAIN. Certbot will ask for a contact email and Terms of Service acceptance."
certbot --nginx --redirect -d "$DOMAIN"

systemctl enable --now samsara-bot.service
systemctl restart samsara-bot.service
systemctl --no-pager --full status samsara-bot.service
echo "Deployment configured. Check https://$DOMAIN/ and the service logs with: journalctl -u samsara-bot -f"
