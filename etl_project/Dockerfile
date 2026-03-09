FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

# ENV API_KEY=<api_key>
# ENV SERVER_NAME=<server_name>
# ENV DATABASE_NAME=weather
# ENV DB_HOST=
# ENV DB_PORT=<password>

CMD ["python", "main.py"]

