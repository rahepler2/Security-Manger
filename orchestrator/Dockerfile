FROM python:3.11-slim

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates git gnupg unzip build-essential jq \
  && rm -rf /var/lib/apt/lists/*

# Install Syft (SBOM generator)
ENV SYFT_VERSION=0.70.0
RUN curl -sSL "https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/syft_${SYFT_VERSION}_linux_amd64.tar.gz" \
  | tar -xz -C /usr/local/bin syft

# Install Trivy client (will use trivy client to call Trivy Server)
ENV TRIVY_VERSION=0.48.0
RUN curl -sSL "https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz" \
  | tar -xz -C /usr/local/bin trivy

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy orchestrator app
COPY app.py .

# Create data dir for SBOMs/reports
RUN mkdir -p /data

ENV PYTHONUNBUFFERED=1

EXPOSE 5000
CMD ["python", "app.py"]
