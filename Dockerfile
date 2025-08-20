# Debian 12 (bookworm) slim base
FROM python:3.13.7-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Create app dir and non-root user early
WORKDIR /opt/app
RUN addgroup --system app && adduser --system --ingroup app --home /opt/app app

# System deps + Microsoft repo (signed-by) + ODBC drivers/tools
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      curl ca-certificates gnupg \
      unixodbc unixodbc-dev \
      libpq5 libpq-dev \
 && mkdir -p /usr/share/keyrings \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/msprod.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/msprod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 mssql-tools \
 # expose tools without editing shell rc files
 && ln -s /opt/mssql-tools/bin/sqlcmd /usr/local/bin/sqlcmd \
 && ln -s /opt/mssql-tools/bin/bcp /usr/local/bin/bcp \
 && rm -rf /var/lib/apt/lists/*

# Temporary build toolchain for packages that may compile on Python 3.13
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential python3-dev \
 && rm -rf /var/lib/apt/lists/*

# Copy only requirements first for better caching; install deps
COPY --chown=app:app requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Remove build-time packages to slim the image
RUN apt-get purge -y --auto-remove build-essential python3-dev libpq-dev unixodbc-dev gnupg \
 && rm -rf /var/lib/apt/lists/*

# Copy the rest of the app as non-root
COPY --chown=app:app . .

# Tighten file permissions (read-only code at runtime)
RUN find /opt/app -type d -exec chmod 0755 {} \; \
 && find /opt/app -type f -exec chmod 0644 {} \; \
 && chmod 0755 /opt/app

USER app

CMD ["python", "main.py"]
