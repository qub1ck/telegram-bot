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

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Get token from environment variables
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
DATABASE_URL = os.environ.get('DATABASE_URL')


# Helper function to run async code from Flask
def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# Initialize database at startup
with app.app_context():
    run_async(initialize_db())


# Function to handle background processing
def process_form_in_background(chat_id, job_name, user_input):
    """Process form submission in background thread to avoid timeouts."""
    try:
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Enhanced logging for debugging
        logger.info(f"Starting background processing for user {chat_id}")
        logger.info(f"Job Name: {job_name}")
        logger.info(
            f"User Input (Partial): {json.dumps({k: v for k, v in user_input.items() if k not in ['password']}, indent=2)}")

        # Save form submission and update job status
        success = loop.run_until_complete(save_form_submission(chat_id, user_input, job_name))

        # Log the result of form submission
        if not success:
            logger.error(f"Failed to save form for user {chat_id}, job {job_name}")

            # Attempt to send an error message to the user
            try:
                error_payload = {
                    "chat_id": chat_id,
                    "text": "Error processing your form. Please try again."
                }
                requests.post(TELEGRAM_API_URL, json=error_payload)
            except Exception as error_send_e:
                logger.error(f"Could not send error message: {str(error_send_e)}")
            return

        # Prepare the message to send back to the user
        message = (
            "Form Submission Received:\n\n"
            f"Parent Identifier:\n"
            f"Volume Page Number: {user_input['volume_page_number']}\n\n"
            f"Child 1:\n"
            f"Child 1 Identifier: {user_input['child1_identifier']}\n"
            f"Name: {user_input['child1_name']}\n"
            f"Birth Date: {user_input['child1_birth_date']}\n"
        )

        # Add Child 2 and Child 3 data if available
        if user_input.get("child2_name"):
            message += (
                f"\nChild 2:\n"
                f"Child 2 Identifier: {user_input.get('child2_identifier', 'N/A')}\n"
                f"Name: {user_input['child2_name']}\n"
                f"Birth Date: {user_input.get('child2_birth_date', 'N/A')}\n"
            )
        if user_input.get("child3_name"):
            message += (
                f"\nChild 3:\n"
                f"Child 3 Identifier: {user_input.get('child3_identifier', 'N/A')}\n"
                f"Name: {user_input['child3_name']}\n"
                f"Birth Date: {user_input.get('child3_birth_date', 'N/A')}\n"
            )

        # Add message about automatic search
        message += "\nRegistration form has been submitted successfully. Your search will start automatically."

        # Send the form confirmation message
        payload = {
            "chat_id": chat_id,
            "text": message,
        }
        response = requests.post(TELEGRAM_API_URL, json=payload)

        if response.status_code != 200:
            logger.error(f"Failed to send confirmation message: {response.text}")
            return

        logger.info(f"Confirmation message sent to user {chat_id}")

        # Send a message that search is starting
        search_message = f"Starting automatic search for {job_name}. I'll notify you when appointments become available."
        search_payload = {
            "chat_id": chat_id,
            "text": search_message,
        }
        search_response = requests.post(TELEGRAM_API_URL, json=search_payload)

        if search_response.status_code != 200:
            logger.error(f"Failed to send search message: {search_response.text}")
        else:
            logger.info(f"Search notification sent to user {chat_id}")

    except Exception as e:
        logger.error(f"Comprehensive error in background processing: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
    finally:
        loop.close()


@app.route("/submit-form", methods=["POST"])
def handle_form_submission():
    # Get form data
    data = request.form

    # Log received form data for debugging
    logger.info("Form Submission Received")
    for key, value in data.items():
        # Mask sensitive information
        if key.lower() == 'password':
            logger.info(f"{key}: ********")
        else:
            logger.info(f"{key}: {value}")

    # Extract user input
    user_input = {
        "volume_page_number": data.get("volumePageNumber"),
        "password": data.get("password"),
        "child1_identifier": data.get("child1Identifier"),
        "child1_name": data.get("child1Name"),
        "child1_birth_date": data.get("child1BirthDate"),
        "child2_identifier": data.get("child2Identifier", ""),
        "child2_name": data.get("child2Name", ""),
        "child2_birth_date": data.get("child2BirthDate", ""),
        "child3_identifier": data.get("child3Identifier", ""),
        "child3_name": data.get("child3Name", ""),
        "child3_birth_date": data.get("child3BirthDate", ""),
    }

    # Get the user's chat ID and job name from the form
    chat_id = data.get("chat_id")
    job_name = data.get("job_name")

    logger.info(f"Received form submission for user {chat_id}, job {job_name}")

    # Validate required parameters
    if not chat_id or not job_name:
        logger.error(f"Missing required parameters: chat_id={chat_id}, job_name={job_name}")
        return jsonify({"status": "error", "message": "Missing chat_id or job_name"}), 400

    try:
        # Quickly upsert the user to ensure they exist
        run_async(upsert_user(chat_id))

        # Start a background thread to process the form submission
        thread = threading.Thread(
            target=process_form_in_background,
            args=(chat_id, job_name, user_input)
        )
        thread.daemon = True
        thread.start()

        logger.info(f"Started background processing for user {chat_id}, job {job_name}")

        # Return success immediately
        return jsonify({
            "status": "success",
            "message": "Form received. Processing in background."
        })

    except Exception as e:
        logger.error(f"Error in handle_form_submission: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": f"Error: {str(e)}"}), 500


if __name__ == "__main__":
    # Use 0.0.0.0 to make the server accessible externally
    app.run(host="0.0.0.0", port=5000)
