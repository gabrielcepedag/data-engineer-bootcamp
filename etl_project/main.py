import os

if __name__ == "__main__":
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    print(f'DB_HOST: {db_host} | DB_PORT: {db_port}')