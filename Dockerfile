FROM node:20-slim AS dashboard
WORKDIR /app/dashboard
COPY dashboard/package.json dashboard/pnpm-lock.yaml ./
RUN npm install -g pnpm && pnpm install --frozen-lockfile
COPY dashboard/ ./
RUN pnpm build

FROM python:3.12-slim
WORKDIR /app
COPY . .
COPY --from=dashboard /app/pgwerk/api/static ./pgwerk/api/static
RUN pip install --no-cache-dir ".[api]"
EXPOSE 8000
CMD ["werk", "api", "--host", "0.0.0.0"]
