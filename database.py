from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# Get database URL from environment with validation
DATABASE_URL = os.environ.get('DATABASE_URL', '')
if not DATABASE_URL:
    logger.error("DATABASE_URL is not set. Please set it in your environment variables.")
    raise ValueError("DATABASE_URL must be provided")

# Ensure proper PostgreSQL connection string
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

# Add SSL mode and connection pooling parameters
if '?' not in DATABASE_URL:
    DATABASE_URL += '?sslmode=require'

try:
    # Enhanced connection pooling and error handling
    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_pre_ping=True,  # Test connections before using
        pool_size=10,        # Number of connections to keep open
        max_overflow=20,     # Additional connections if pool is full
        pool_timeout=30,     # Wait time for getting a connection
        pool_recycle=1800,   # Recycle connections after 30 minutes
    )
    
    # Create thread-local session factory
    SessionLocal = scoped_session(sessionmaker(bind=engine))
except Exception as e:
    logger.error(f"Database connection error: {e}")
    logger.error(f"DATABASE_URL: {DATABASE_URL}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    raise

def init_db():
    """Optimized table creation with error handling and indexing."""
    try:
        with engine.begin() as conn:
            # Users table with additional indexing
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE NOT NULL,
                    last_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
            """))

            # User jobs table with performance optimization
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_jobs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    job_name TEXT NOT NULL,
                    status TEXT DEFAULT 'pending_form',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(user_id) REFERENCES users(user_id),
                    UNIQUE(user_id, job_name)
                );
                CREATE INDEX IF NOT EXISTS idx_user_jobs_user_id ON user_jobs(user_id);
                CREATE INDEX IF NOT EXISTS idx_user_jobs_status ON user_jobs(status);
            """))

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
                    preferred_date TEXT,  -- Add this field
                    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );
            """))

        logger.info("Database tables created with optimized indexing.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
