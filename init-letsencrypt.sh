#!/bin/bash
set -e

DOMAIN="powerbi.mozaicweb.com"
EMAIL="shayan@mozaicweb.com"

CERT_PATH="./certbot/conf/live/$DOMAIN"

mkdir -p ./certbot/conf ./certbot/www

# If real cert already exists, just start everything
if [ -f "$CERT_PATH/fullchain.pem" ]; then
    echo "Certificate already exists. Starting services..."
    docker compose up -d
    exit 0
fi

# Create a temporary dummy cert so nginx can start without crashing
echo "Creating temporary self-signed certificate..."
mkdir -p "$CERT_PATH"
docker run --rm \
    -v "$(pwd)/certbot/conf:/etc/letsencrypt" \
    alpine/openssl req -x509 -nodes -newkey rsa:2048 -days 1 \
    -keyout "/etc/letsencrypt/live/$DOMAIN/privkey.pem" \
    -out    "/etc/letsencrypt/live/$DOMAIN/fullchain.pem" \
    -subj   "/CN=$DOMAIN" 2>/dev/null

# Start nginx (and dependencies) with the dummy cert
echo "Starting nginx with temporary certificate..."
docker compose up -d nginx web db redis worker beat
echo "Waiting for nginx to be ready..."
sleep 5

# Get the real Let's Encrypt certificate
echo "Requesting Let's Encrypt certificate for $DOMAIN..."
docker compose run --rm --no-deps certbot certonly \
    --webroot \
    --webroot-path=/var/www/certbot \
    --email "$EMAIL" \
    --agree-tos \
    --no-eff-email \
    --force-renewal \
    -d "$DOMAIN"

# Reload nginx with the real cert
echo "Reloading nginx with real certificate..."
docker compose exec nginx nginx -s reload

# Start remaining services
docker compose up -d

echo ""
echo "Done! App is live at https://$DOMAIN"
