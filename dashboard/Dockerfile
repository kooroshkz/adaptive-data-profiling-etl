# Dashboard: Next.js app that queries local parquet through a small Python/DuckDB
# helper (scripts/s3_query.py). The image therefore ships both Node and Python.
FROM node:20-bookworm-slim

# Python + DuckDB for the query helper.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir --break-system-packages duckdb

WORKDIR /app

# Install node deps first (better layer caching).
COPY package.json package-lock.json* ./
RUN npm install

# App source.
COPY . .

# The query helper runs via this interpreter (see src/lib/pythonRunner.ts).
ENV PYTHON_BIN=python3
ENV NODE_ENV=development

EXPOSE 3000
# Bind to 0.0.0.0 so the app is reachable from the host.
CMD ["npm", "run", "dev", "--", "-H", "0.0.0.0", "-p", "3000"]
