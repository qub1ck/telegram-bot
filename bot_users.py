import logging
import traceback
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from database import SessionLocal, init_db

logger = logging.getLogger(__name__)


async def initialize_db():
    """Initialize the database."""
    try:
        init_db()
        logger.info("Database tables created successfully.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")


async def upsert_user(user_id):
    """Insert or update a user's last interaction timestamp."""
    try:
        with SessionLocal() as session:
            # Use raw SQL to handle upsert
            session.execute(text("""
                INSERT INTO users (user_id)
                VALUES (:user_id)
                ON CONFLICT (user_id) DO UPDATE
                SET last_interaction = CURRENT_TIMESTAMP;
            """), {"user_id": user_id})
            session.commit()
            logger.info(f"User {user_id} upserted in the 'users' table.")
    except SQLAlchemyError as e:
        logger.error(f"Error upserting user {user_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")


async def save_form_submission(user_id, form_data, job_name):
    """Save form submission data to the database and update job status."""
    try:
        with SessionLocal() as session:
            # Insert form submission
            session.execute(text("""
                INSERT INTO form_submissions (
                    user_id, volume_page_number, password,
                    child1_identifier, child1_name, child1_birth_date,
                    child2_identifier, child2_name, child2_birth_date,
                    child3_identifier, child3_name, child3_birth_date,
                    job_name, preferred_date
                ) VALUES (
                    :user_id, :volume_page_number, :password,
                    :child1_identifier, :child1_name, :child1_birth_date,
                    :child2_identifier, :child2_name, :child2_birth_date,
                    :child3_identifier, :child3_name, :child3_birth_date,
                    :job_name, :preferred_date
                )
            """), {
                "user_id": user_id,
                "volume_page_number": form_data.get("volume_page_number"),
                "password": form_data.get("password"),
                "child1_identifier": form_data.get("child1_identifier"),
                "child1_name": form_data.get("child1_name"),
                "child1_birth_date": form_data.get("child1_birth_date"),
                "child2_identifier": form_data.get("child2_identifier", ""),
                "child2_name": form_data.get("child2_name", ""),
                "child2_birth_date": form_data.get("child2_birth_date", ""),
                "child3_identifier": form_data.get("child3_identifier", ""),
                "child3_name": form_data.get("child3_name", ""),
                "child3_birth_date": form_data.get("child3_birth_date", ""),
                "job_name": job_name,
                "preferred_date": form_data.get("preferred_date", "")
            })

            # Update job status
            session.execute(text("""
                UPDATE user_jobs
                SET status = 'active'
                WHERE user_id = :user_id AND job_name = :job_name
            """), {"user_id": user_id, "job_name": job_name})

            session.commit()
            logger.info(f"Form submission saved for user {user_id}, job {job_name}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Error saving form submission: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


async def add_user_job(user_id, job_name):
    """Add a new job for a user with pending_form status."""
    try:
        await upsert_user(user_id)
        with SessionLocal() as session:
            session.execute(text("""
                INSERT INTO user_jobs (user_id, job_name, status)
                VALUES (:user_id, :job_name, 'pending_form')
            """), {"user_id": user_id, "job_name": job_name})
            session.commit()
            logger.info(f"Job {job_name} added for user {user_id} with pending_form status.")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Error adding user job: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


async def is_job_ready_to_search(user_id, job_name):
    """Check if a job is ready to start searching (form submitted)."""
    try:
        with SessionLocal() as session:
            result = session.execute(text("""
                SELECT status FROM user_jobs
                WHERE user_id = :user_id AND job_name = :job_name
            """), {"user_id": user_id, "job_name": job_name}).fetchone()

            if result:
                logger.info(f"Job status found: {result[0]}")
                return result[0] == 'active'
            else:
                logger.warning(f"No job found for user {user_id}, job {job_name}")
                return False
    except SQLAlchemyError as e:
        logger.error(f"Error checking job readiness: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


async def get_all_active_jobs():
    """Fetch all users with active jobs from the database."""
    try:
        with SessionLocal() as session:
            results = session.execute(text("""
                SELECT user_id, job_name FROM user_jobs
                WHERE status = 'active'
            """)).fetchall()

            logger.info(f"Active jobs retrieved from database: {results}")
            return [{"user_id": row[0], "job_name": row[1]} for row in results]
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving active jobs: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []


async def remove_user_job(user_id, job_name):
    """Remove a job for a user."""
    try:
        with SessionLocal() as session:
            session.execute(text("""
                DELETE FROM user_jobs WHERE user_id = :user_id AND job_name = :job_name
            """), {"user_id": user_id, "job_name": job_name})
            session.commit()
            logger.info(f"Job {job_name} removed for user {user_id}.")
    except SQLAlchemyError as e:
        logger.error(f"Error removing user job: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")


async def get_preferred_date(user_id, job_name):
    """Get the preferred date for a job."""
    try:
        with SessionLocal() as session:
            result = session.execute(text("""
                SELECT preferred_date FROM form_submissions
                WHERE user_id = :user_id AND job_name = :job_name
                ORDER BY submitted_at DESC
                LIMIT 1
            """), {"user_id": user_id, "job_name": job_name}).fetchone()

            if result and result[0]:
                return result[0]
            return None
    except SQLAlchemyError as e:
        logger.error(f"Error getting preferred date: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

async def get_user_jobs(user_id):
    """Get all jobs for a user."""
    try:
        with SessionLocal() as session:
            results = session.execute(text("""
                SELECT job_name FROM user_jobs WHERE user_id = :user_id
            """), {"user_id": user_id}).fetchall()
            return [row[0] for row in results]
    except SQLAlchemyError as e:
        logger.error(f"Error getting user jobs: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

async def update_preferred_date(user_id, job_name, preferred_date):
    """Update preferred date for an existing job."""
    try:
        with SessionLocal() as session:
            # Check if the job already has a form submission
            existing = session.execute(text("""
                SELECT id FROM form_submissions
                WHERE user_id = :user_id AND job_name = :job_name
                LIMIT 1
            """), {"user_id": user_id, "job_name": job_name}).fetchone()
            
            if existing:
                # Update existing record
                session.execute(text("""
                    UPDATE form_submissions
                    SET preferred_date = :preferred_date
                    WHERE user_id = :user_id AND job_name = :job_name
                """), {"user_id": user_id, "job_name": job_name, "preferred_date": preferred_date})
            else:
                # Create a minimal record
                session.execute(text("""
                    INSERT INTO form_submissions (user_id, job_name, preferred_date)
                    VALUES (:user_id, :job_name, :preferred_date)
                """), {"user_id": user_id, "job_name": job_name, "preferred_date": preferred_date})
            
            session.commit()
            logger.info(f"Updated preferred date for user {user_id}, job {job_name}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Error updating preferred date: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False
