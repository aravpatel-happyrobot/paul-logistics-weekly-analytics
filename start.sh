#!/bin/sh
# Start script for Railway deployment
exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
