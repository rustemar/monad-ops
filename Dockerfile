FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY monad_ops/ monad_ops/
COPY labels.json .

RUN pip install --no-cache-dir .

# Default config path — mount your own at runtime.
ENV MONAD_OPS_CONFIG=/app/config.toml

EXPOSE 8873

CMD ["monad-ops", "run"]
