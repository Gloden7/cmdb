from sqlalchemy import Column, DateTime, Boolean, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, ForeignKey, func
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from settings import MYSQL_URI


engine = create_engine(MYSQL_URI, echo=False)
Base = declarative_base()
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


class BaseModel:
    createtime = Column(DateTime, default=datetime.now)
    updatetime = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    is_delete = Column(Boolean, default=False)


class Schema(BaseModel, Base):
    __tablename__ = "schema"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(48), nullable=False)
    desc = Column(String(128))


class Field(BaseModel, Base):
    __tablename__ = "field"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(48), nullable=False)
    meta = Column(Text)
    ref = Column(Integer)
    desc = Column(String(128))
    schema_id = Column(Integer, ForeignKey("schema.id"), nullable=False)
    values = relationship("Value", backref="field")


class Entity(BaseModel, Base):
    __tablename__ = "entity"
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(48), nullable=False)
    schema_id = Column(Integer, ForeignKey("schema.id"), nullable=False)
    values = relationship("Value", backref="entity")

    def todict(self):
        return dict(id=self.id, key=self.key, createtime=self.createtime.timestamp(), updatetime=self.createtime.timestamp())


class Value(BaseModel, Base):
    __tablename__ = "value"
    id = Column(Integer, primary_key=True, autoincrement=True)
    value = Column(Text)
    field_id = Column(Integer, ForeignKey("field.id"), nullable=False)
    entity_id = Column(Integer, ForeignKey("entity.id"), nullable=False)

    def todict(self):
        return dict(id=self.id, value=self.value)


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    # Base.metadata.drop_all(engine)