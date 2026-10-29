FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PORT=5000 \
    DB_PATH=/app/data/users.db

COPY web_ui.py /app/
COPY assets/ /app/assets/

RUN mkdir -p /app/data

EXPOSE 5000

CMD ["python", "web_ui.py"]
