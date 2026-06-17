FROM python:3.12-slim

# system deps needed by GDAL / rasterio
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# install Python deps first (layer caching — only rebuilds if pyproject.toml changes)
COPY pyproject.toml .
RUN pip install --upgrade pip setuptools && pip install -e ".[dev]"

# copy source after deps (so code changes don't invalidate the dep layer)
COPY src/ ./src/
COPY run.py .
COPY Makefile .

# create data dirs
RUN mkdir -p data/raw data/processed

CMD ["python", "run.py"]
