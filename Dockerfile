FROM python:3.12 as python-base

# https://python-poetry.org/docs#ci-recommendations
ENV POETRY_VERSION=1.7.1
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv

# Tell Poetry where to place its cache and virtual environment
ENV POETRY_CACHE_DIR=/opt/.cache

# Create stage for Poetry installation
FROM python-base as poetry-base

# Creating a virtual environment just for poetry and install it with pip
RUN python3 -m venv $POETRY_VENV \
	&& $POETRY_VENV/bin/pip install -U pip setuptools \
	&& $POETRY_VENV/bin/pip install poetry==${POETRY_VERSION}

# Create a new stage from the base python image
FROM python-base as example-app

# Copy Poetry to app image
COPY --from=poetry-base ${POETRY_VENV} ${POETRY_VENV}

# Add Poetry to PATH
ENV PATH="${PATH}:${POETRY_VENV}/bin"

WORKDIR /app
RUN chmod +x /app

# Copy Dependencies
COPY poetry.lock pyproject.toml README.md ./

# [OPTIONAL] Validate the project is properly configured
RUN poetry check

# Install Dependencies
RUN poetry install && poetry show

# Copy Application
COPY . /app

# Run Application
CMD poetry run python main.py -p sample/sample.txt -u https://httpbin.org/post -l 2 -o output/output.txt --vcpu 1 --filter parent.surname==Brown