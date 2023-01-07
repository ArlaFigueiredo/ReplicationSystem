from sqlalchemy import create_engine


def execute_sql(idf: int, sql_raw: str):
    engine = create_engine(f"sqlite:///database{idf}.db")
    conn = engine.connect()
    trans = conn.begin()
    conn.execute(sql_raw)
    trans.commit()
