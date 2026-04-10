# Deployment Guide

This guide covers building, running, and deploying MCP LogBench behind a reverse proxy.

## Building the Image

```bash
docker build -t mcp-logbench .
```

## Runtime Configuration

The application requires:

1. **A config file** mounted into the container at a path specified by `CONFIG_PATH`
2. **Secret env vars** injected at runtime (never baked into the image)

Copy `config.example.yaml` and fill in your values (or reference env vars via `${VAR_NAME}`):

```bash
cp config.example.yaml config.yaml
# Edit config.yaml -- set URLs, org IDs, dataset names
# Leave token values as ${VAR_NAME} references
```

> **Important:** `server.host` in your config must be `0.0.0.0`, not `127.0.0.1` or
> `localhost`. Inside the container, the application binds to that address. If you
> bind to loopback only, Docker cannot forward traffic from the host port and the
> container will be unreachable.

## Running the Container

```bash
docker run -d \
  --name mcp-logbench \
  -p 8080:8080 \
  -v ./config.yaml:/app/config.yaml:ro \
  -e CONFIG_PATH=/app/config.yaml \
  -e AXIOM_PROD_TOKEN=your-token-here \
  -e AXIOM_STAGING_TOKEN=your-staging-token \
  -e AZURE_TENANT_ID=your-tenant-id \
  -e AZURE_CLIENT_ID=your-client-id \
  mcp-logbench
```

The container runs as UID 65532 (nonroot). All secrets are injected via `-e` -- nothing is stored in the image.

## Reverse Proxy Setup

MCP LogBench listens on port 8080. TLS termination and authentication header forwarding must be handled by the reverse proxy.

The proxy must forward these headers:

- `Host`
- `X-Forwarded-For`
- `X-Forwarded-Proto`

### nginx

```nginx
server {
    listen 443 ssl;
    server_name mcp.example.com;

    ssl_certificate     /etc/ssl/certs/mcp.example.com.crt;
    ssl_certificate_key /etc/ssl/private/mcp.example.com.key;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Caddy

```caddyfile
mcp.example.com {
    reverse_proxy localhost:8080
}
```

Caddy automatically handles TLS and sets the forwarded headers.

## Verifying the Deployment

**Check the container runs as non-root:**

```bash
docker run --rm --entrypoint id mcp-logbench
# Expected: uid=65532(nonroot) gid=65532(nonroot) groups=65532(nonroot)
```

**Check logs are JSON:**

```bash
docker logs mcp-logbench 2>&1 | head -1 | python3 -m json.tool
```

**Check no secrets in image layers:**

```bash
docker history --no-trunc mcp-logbench | grep -i -E "token|secret|password" \
  && echo "FAIL: secrets found" || echo "PASS: no secrets in layers"
```

## Log Configuration

| Env var | Default | Values |
|---------|---------|--------|
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_FORMAT` | `json` | `json`, `text` |

Logs are written to stdout. In production, leave `LOG_FORMAT=json` (the default) for structured log ingestion.
