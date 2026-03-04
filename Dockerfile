FROM python:3.9

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY /app .

ENV API_KEY=<api_key>
ENV SERVER_NAME=<server_name>
ENV DATABASE_NAME=weather
ENV DB_USERNAME=postgres
ENV DB_PASSWORD=<password>
ENV PORT=5432

CMD ["python", "-m", "etl_project.pipelines"]