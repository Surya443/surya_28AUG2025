from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

import os

DATABASE_URL = os.getenv("DATABASE_URL")

def test_connection():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            print("success")
    except OperationalError as e:
        print("fail")
        print(e)

if __name__ == "__main__":
    test_connection()