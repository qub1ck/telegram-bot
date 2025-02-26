from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# Get database URL from environment with a fallback
DATABASE_URL = os.environ.get('DATABASE_URL')

# Validate DATABASE_URL
if not DATABASE_URL:
    logger.error("DATABASE_URL is not set. Please set it in your environment variables.")
    raise ValueError("DATABASE_URL must be provided")

# Ensure the connection string is in the correct format for SQLAlchemy
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

# Add SSL mode if not present
if '?' not in DATABASE_URL:
    DATABASE_URL += '?sslmode=require'

# Create engine with connection pooling and error handling
try:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,  # Test connections before using them
        pool_size=10,  # Number of connections to keep open
        max_overflow=20  # Number of connections that can be created beyond pool_size
    )
    SessionLocal = sessionmaker(bind=engine)
except Exception as e:
    logger.error(f"Database connection error: {e}")
    logger.error(f"DATABASE_URL: {DATABASE_URL}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    raise

def init_db():
    """Create all tables in the database."""
    try:
        # Create tables if they don't exist
        with engine.begin() as conn:
            # Users table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE NOT NULL,
                    last_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))

            # User jobs table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_jobs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    job_name TEXT NOT NULL,
                    status TEXT DEFAULT 'pending_form',
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );
            """))

            # Form submissions table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS form_submissions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    volume_page_number TEXT,
                    password TEXT,
                    child1_identifier TEXT,
                    child1_name TEXT,
                    child1_birth_date TEXT,
                    child2_identifier TEXT,
                    child2_name TEXT,
                    child2_birth_date TEXT,
                    child3_identifier TEXT,
                    child3_name TEXT,
                    child3_birth_date TEXT,
                    job_name TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );
            """))

        logger.info("Database tables created or already exist.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
