from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import os
import asyncio
import logging
import threading
import json
import traceback
from dotenv import load_dotenv
from sqlalchemy import text
from error_logger import log_error, send_user_friendly_message

# Import the async database functions
from bot_users import initialize_db, upsert_user, save_form_submission
from database import SessionLocal

# Load environment variables
load_dotenv()

# Setup logging with rotation and more detailed configuration
import logging
from logging.handlers import RotatingFileHandler

def setup_logging():
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)

    # File handler with rotation
    file_handler = RotatingFileHandler('app.log', maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)

    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        handlers=[console_handler, file_handler]
    )

setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Validate and retrieve configuration
def get_config(key, default=None, required=False):
    """Safely retrieve configuration with optional validation."""
    value = os.environ.get(key, default)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {key}")
    return value

# Get critical configuration
TELEGRAM_BOT_TOKEN = get_config('TELEGRAM_BOT_TOKEN', required=True)
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
DATABASE_URL = get_config('DATABASE_URL', required=True)

# Helper function to run async code from Flask with enhanced error handling
def run_async(coro, timeout=30):
    """Run async coroutine with timeout and comprehensive error handling."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(asyncio.wait_for(coro, timeout))
    except asyncio.TimeoutError:
        logger.error(f"Async operation timed out after {timeout} seconds")
        raise
    except Exception as e:
        logger.error(f"Error in async operation: {e}")
        raise
    finally:
        loop.close()

# Initialize database at startup with retry mechanism
def initialize_database():
    """Initialize database with retry logic."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            run_async(initialize_db())
            logger.info("Database initialized successfully.")
            return
        except Exception as e:
            logger.error(f"Database initialization attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.critical("Failed to initialize database after multiple attempts.")
                raise

# Call initialization at module import
initialize_database()

# Background processing with improved error tracking
def process_form_in_background(chat_id, job_name, user_input):
    """Enhanced background processing with comprehensive logging and error handling."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Redact sensitive information in logs
        log_safe_input = {k: v for k, v in user_input.items() if k not in ['password']}
        logger.info(f"Background processing started: User {chat_id}, Job {job_name}")
        logger.debug(f"User Input (Sanitized): {json.dumps(log_safe_input, indent=2)}")

        # Save form submission with timeout
        success = loop.run_until_complete(
            asyncio.wait_for(save_form_submission(chat_id, user_input, job_name), timeout=60)
        )

        if not success:
            logger.error(f"Form submission failed for user {chat_id}, job {job_name}")
            _send_error_message(chat_id, "Form submission processing error")
            return

        # Construct detailed message with minimal sensitive information
        message = _construct_submission_message(user_input)
        _send_telegram_message(chat_id, message)
        _send_search_start_message(chat_id, job_name)

    except Exception as e:
        logger.error(f"Comprehensive error in background processing: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        _send_error_message(chat_id, "Unexpected error during processing")
    finally:
        loop.close()

def _send_telegram_message(chat_id, message):
    """Send Telegram message with robust error handling."""
    try:
        response = requests.post(TELEGRAM_API_URL, json={"chat_id": chat_id, "text": message}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to send Telegram message: {e}")

def _send_error_message(chat_id, error_text):
    """Send a standardized error message."""
    try:
        # Log error to monitoring bot instead of showing details to user
        log_error(chat_id, error_text)
        
        # Send generic message to user
        send_user_friendly_message(
            TELEGRAM_BOT_TOKEN, 
            chat_id
        )
    except requests.RequestException as e:
        logger.error(f"Failed to handle error messaging: {e}")

def _construct_submission_message(user_input):
    """Construct a safe submission message."""
    message = "Form Submission Received:\n\n"
    message += f"Parent Identifier:\nVolume Page Number: {user_input.get('volume_page_number', 'N/A')}\n\n"
    
    # Dynamically handle children based on available data
    for i in range(1, 4):
        child_prefix = f"child{i}_"
        if user_input.get(f"{child_prefix}name"):
            message += f"Child {i}:\n"
            message += f"Identifier: {user_input.get(f'{child_prefix}identifier', 'N/A')}\n"
            message += f"Name: {user_input.get(f'{child_prefix}name', 'N/A')}\n"
            message += f"Birth Date: {user_input.get(f'{child_prefix}birth_date', 'N/A')}\n\n"
    
    message += "Registration form submitted successfully. Automatic search will start."
    return message

def _send_search_start_message(chat_id, job_name):
    """Send a search start notification."""
    try:
        requests.post(
            TELEGRAM_API_URL, 
            json={"chat_id": chat_id, "text": f"Starting automatic search for {job_name}."},
            timeout=10
        )
    except requests.RequestException as e:
        logger.error(f"Failed to send search start message: {e}")


@app.route("/submit-form", methods=["POST"])
def handle_form_submission():
    """Robust form submission handler with comprehensive validation and processing."""
    try:
        # Extract and validate form data
        data = request.form
        logger.info("Form Submission Received")

        # Validate critical parameters
        chat_id = data.get("chat_id")
        job_name = data.get("job_name")
        service_type = data.get("service_type", "menores")

        if not chat_id or not job_name:
            logger.error(f"Missing parameters: chat_id={chat_id}, job_name={job_name}")
            return jsonify({"status": "error", "message": "Missing required parameters"}), 400

        # Create a mapping for form field names to database column names
        field_mapping = {
            # Standard menores fields
            'volumePageNumber': 'volume_page_number',
            'password': 'password',
            'child1Identifier': 'child1_identifier',
            'child1Name': 'child1_name',
            'child1BirthDate': 'child1_birth_date',
            'child2Identifier': 'child2_identifier',
            'child2Name': 'child2_name',
            'child2BirthDate': 'child2_birth_date',
            'child3Identifier': 'child3_identifier',
            'child3Name': 'child3_name',
            'child3BirthDate': 'child3_birth_date',

            # Certificate fields
            'carneIdentidad': 'carne_identidad',
            'contrasena': 'contrasena',
            'tomo': 'tomo',
            'pagina': 'pagina',
            'visado_mark': 'visado_mark',
        }

        # Transform form data using the mapping
        user_input = {}
        for key, value in data.items():
            if key in field_mapping:
                user_input[field_mapping[key]] = value
            elif key.lower() not in ['chat_id', 'job_name', 'service_type']:
                # For any fields not in our mapping, use the original transformation
                user_input[key.lower().replace(" ", "_")] = value

        # Add service type
        user_input['service_type'] = service_type

        # Log the transformed data for debugging (excluding password)
        safe_input = {k: v for k, v in user_input.items() if k not in ['password', 'contrasena']}
        logger.info(f"Transformed form data: {safe_input}")

        # Quick user upsert to ensure user exists
        run_async(upsert_user(chat_id))

        # Start background processing
        thread = threading.Thread(
            target=process_form_in_background,
            args=(chat_id, job_name, user_input)
        )
        thread.daemon = True
        thread.start()

        logger.info(f"Background processing initiated: User {chat_id}, Job {job_name}")

        return jsonify({
            "status": "success",
            "message": "Form received and processing started."
        })

    except Exception as e:
        logger.error(f"Form submission error: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


@app.route("/get-form-data", methods=["GET"])
def get_form_data():
    """Get form data for a specific user's job."""
    try:
        # Extract and validate parameters
        chat_id = request.args.get("chat_id")
        job_name = request.args.get("job_name")

        if not chat_id or not job_name:
            logger.error(f"Missing required parameters: chat_id={chat_id}, job_name={job_name}")
            return jsonify({"status": "error", "message": "Missing required parameters"}), 400

        # First get the service type from the user_jobs table
        with SessionLocal() as session:
            service_type_result = session.execute(text("""
                SELECT service_type FROM user_jobs
                WHERE user_id = :user_id AND job_name = :job_name
                LIMIT 1
            """), {"user_id": chat_id, "job_name": job_name}).fetchone()

            if not service_type_result:
                return jsonify({"status": "error", "message": "No job found"}), 404

            service_type = service_type_result[0]

            if service_type == "menores":
                # Query for menores service data
                result = session.execute(text("""
                    SELECT volume_page_number, password, 
                           child1_identifier, child1_name, child1_birth_date,
                           child2_identifier, child2_name, child2_birth_date,
                           child3_identifier, child3_name, child3_birth_date,
                           preferred_date
                    FROM menores_submissions
                    WHERE user_id = :user_id AND job_name = :job_name
                    ORDER BY submitted_at DESC
                    LIMIT 1
                """), {"user_id": chat_id, "job_name": job_name}).fetchone()

                if not result:
                    return jsonify({"status": "error", "message": "No form data found"}), 404

                # Create a dictionary from the result
                form_data = {
                    "volume_page_number": result[0],
                    "password": result[1],
                    "child1_identifier": result[2],
                    "child1_name": result[3],
                    "child1_birth_date": result[4],
                    "child2_identifier": result[5],
                    "child2_name": result[6],
                    "child2_birth_date": result[7],
                    "child3_identifier": result[8],
                    "child3_name": result[9],
                    "child3_birth_date": result[10],
                    "preferred_date": result[11],
                    "service_type": service_type
                }
            else:
                # Query for certificate service data
                result = session.execute(text("""
                    SELECT carne_identidad, contrasena, tomo, pagina, visado_mark, preferred_date, cert_type
                    FROM certificate_submissions
                    WHERE user_id = :user_id AND job_name = :job_name
                    ORDER BY submitted_at DESC
                    LIMIT 1
                """), {"user_id": chat_id, "job_name": job_name}).fetchone()

                if not result:
                    return jsonify({"status": "error", "message": "No form data found"}), 404

                # Create a dictionary from the result
                form_data = {
                    "carne_identidad": result[0],
                    "contrasena": result[1],
                    "tomo": result[2],
                    "pagina": result[3],
                    "visado_mark": result[4],
                    "preferred_date": result[5],
                    "cert_type": result[6],
                    "service_type": service_type
                }

            # Return the form data
            return jsonify({
                "status": "success",
                "form_data": form_data
            })

    except Exception as e:
        logger.error(f"Error retrieving form data: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(get_config('PORT', 5000)))
