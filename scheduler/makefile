# Makefile for testing Airflow custom operators

PROJECT_ROOT := $(shell pwd)

# Path to tests
TEST_PATH := tests

# Python path to include plugins, dags, etc.
PYTHONPATH := $(PROJECT_ROOT)

# Configuration
AIRFLOW_VERSION=2.8.1
PYTHON_VERSION=3.8
CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt
CONSTRAINT_FILE=constraints.txt

# Commandes
.PHONY: help install-runtime install-dev install-all test lint clean

help:
	@echo ""
	@echo "📘 Airflow Project - Makefile Commands"
	@echo ""
	@echo "  make install-runtime     Install runtime dependencies (Airflow, etc.)"
	@echo "  make install-dev         Install dev dependencies (pytest, flake8, etc.)"
	@echo "  make install-all         Install everything (runtime + dev)"
	@echo "  make test                Run tests with pytest"
	@echo "  make lint                Run flake8, black, isort"
	@echo "  make clean               Remove .pyc and __pycache__"
	@echo ""

install-runtime:
	@echo "📦 Installing runtime dependencies..."
	pip install -r requirements-runtime.txt

install-dev:
	@echo "🛠️ Installing dev dependencies..."
	pip install -r requirements-development.txt

install-all: install-runtime install-dev

test:
	@echo "🧪 Running tests with pytest..."
	pytest

lint:
	@echo "🧹 Running flake8, black, and isort..."
	flake8 .
	black --check .
	isort --check .

clean:
	@echo "🧽 Cleaning up .pyc files..."
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -type d -exec rm -r {} +
