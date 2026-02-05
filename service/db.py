from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import os
from .models import Base

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trivy:trivy_pass@postgres:5432/trivydb")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))

def create_tables():
    Base.metadata.create_all(bind=engine)
