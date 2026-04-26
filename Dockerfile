FROM python:3.12-slim

# journalctl ships in the systemd package; monad-ops tails the host's
# journald and needs the binary at runtime. Install before pip so the
# layer cache invalidates less often.
RUN apt-get update \
 && apt-get install -y --no-install-recommends systemd \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
COPY monad_ops/ monad_ops/
COPY labels.json .

RUN pip install --no-cache-dir .

# Default config path — mount your own at runtime.
ENV MONAD_OPS_CONFIG=/app/config.toml

EXPOSE 8873

# Bind to 0.0.0.0 inside the container so Docker's port mapping
# (-p 127.0.0.1:8873:8873) actually exposes the dashboard. The host
# is what gates external access, not the container's listen interface.
CMD ["monad-ops", "run", "--host", "0.0.0.0"]
