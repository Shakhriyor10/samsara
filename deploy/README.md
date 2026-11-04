# Ubuntu deployment: `baw-sam.uz`

The bot listens on `127.0.0.1:8080`. Nginx is the public HTTPS endpoint and Certbot obtains and renews the certificate. The app and state are separated: code and its existing virtual environment live in `/var/www/samsara`, secrets in `/etc/samsara-bot.env`, and SQLite/PTI photos in `/var/lib/samsara`.

## Before connecting

- Create an `A` record for `baw-sam.uz` pointing to `77.83.206.140`.
- Remove any stale `AAAA` record unless IPv6 is configured on the server.
- Allow inbound TCP ports 22, 80, and 443 in the server/provider firewall.
- The server should run a supported Ubuntu release with systemd and root/sudo access.

## Upload

The project is already expected at `/var/www/samsara`. Include `main.py`, `miniapp_server.py`, `pti_checklist.py`, `pti_reports.py`, `requirements.txt`, `webapp/`, `deploy/`, the existing `.venv/`, and the populated `.env`. Do not upload `__pycache__/` or local logs.

To preserve current configuration and PTI records, keep `samsara_bot.sqlite3` and `pti_uploads/` in `/var/www/samsara/` as temporary staging files. The setup script imports them into `/var/lib/samsara` on first setup and leaves an existing server database intact on reruns. The database contains fleet configuration and Telegram IDs; the photo directory contains private driver images. Transfer them only over SSH/SFTP.

## Configure

Then run as root on the server:

```bash
sudo bash /var/www/samsara/deploy/configure-ubuntu.sh
```

The script checks that DNS already resolves to the target IP, installs Python, FFmpeg, and Nginx, prepares the systemd service, updates `WEBAPP_URL` to `https://baw-sam.uz/`, and runs Certbot. It refuses to replace another active Nginx site for this domain or reuse a site pointing to a different upstream. Certbot prompts for a contact email and Terms of Service acceptance. The script intentionally does not change the provider firewall rules.

After successful startup, open `/app` in the bot and check PTI camera access through the HTTPS domain. Useful commands:

```bash
sudo systemctl status samsara-bot
sudo journalctl -u samsara-bot -f
sudo nginx -t
sudo certbot renew --dry-run
```
