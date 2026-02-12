# FROM python:3.13.11-slim

# WORKDIR /code

# RUN pip install pandas pyarrow  # without uv

# COPY pipeline.py .

# ENTRYPOINT ["python" , "pipeline.py"]


FROM python:3.13.10-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/  
# only the last argument is destination

WORKDIR /code

# Prepand the project venv to PATH
ENV PATH="/code/.venv/bin:$PATH"
# this can remove "uv", "run" from ENTRYPOINT

# Copy only dependency metadata first for layer caching
COPY pyproject.toml uv.lock .python-version ./

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# Copy application code
COPY pipeline.py .

# Set entry point
ENTRYPOINT ["python", "pipeline.py"]