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

# Import the async database functions
from bot_users import initialize_db, upsert_user, save_form_submission

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
        requests.post(
            TELEGRAM_API_URL, 
            json={"chat_id": chat_id, "text": f"Error: {error_text}. Please try again."},
            timeout=10
        )
    except requests.RequestException as e:
        logger.error(f"Failed to send error message: {e}")

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
        
        if not chat_id or not job_name:
            logger.error(f"Missing parameters: chat_id={chat_id}, job_name={job_name}")
            return jsonify({"status": "error", "message": "Missing required parameters"}), 400

        # Create a mapping for form field names to database column names
        field_mapping = {
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
        }
        
        # Transform form data using the mapping
        user_input = {}
        for key, value in data.items():
            if key in field_mapping:
                user_input[field_mapping[key]] = value
            elif key.lower() not in ['chat_id', 'job_name']:
                # For any fields not in our mapping, use the original transformation
                user_input[key.lower().replace(" ", "_")] = value
        
        # Log the transformed data for debugging (excluding password)
        safe_input = {k: v for k, v in user_input.items() if k != 'password'}
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(get_config('PORT', 5000)))
