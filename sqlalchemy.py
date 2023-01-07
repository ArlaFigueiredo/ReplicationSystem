from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer,Float
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///:memory:', echo=True)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

class User(Base):
    __tablename__ = 'ContaBancaria'

    id = Column(Integer, primary_key=True)
    number_conta = Column(Integer, nullable=False)
    value = Column(Float, nullable=False)

    def __repr__(self):
        return f'User [Numero da conta={self.number_conta}, Saldo={self.value}]'

#SQL-SELECT
data = session.query(User).all()
print(data)
print(data[0].id)