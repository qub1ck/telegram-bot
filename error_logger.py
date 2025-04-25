import logging
import traceback
import requests
import os
from datetime import datetime

logger = logging.getLogger(__name__)

# Get token from environment variables or default to None
ERROR_BOT_TOKEN = os.environ.get('ERROR_BOT_TOKEN')
ERROR_CHAT_ID = os.environ.get('ERROR_CHAT_ID')

def log_error(user_id, error_message, job_name=None, additional_info=None):
    """
    Log error to a monitoring bot instead of sending to the user.
    
    Args:
        user_id (str): The ID of the user who experienced the error
        error_message (str): Brief error description
        job_name (str, optional): The job name where the error occurred
        additional_info (dict, optional): Any additional context for debugging
    """
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        logger.warning("ERROR_BOT_TOKEN or ERROR_CHAT_ID not set. Error monitoring is disabled.")
        return False
    
    try:
        # Format current time
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Get traceback information
        stack_trace = traceback.format_exc()
        if stack_trace == "NoneType: None\n":
            stack_trace = "No traceback available"
            
        # Construct message
        message = f"🚨 *ERROR REPORT* 🚨\n"
        message += f"👤 *User ID*: `{user_id}`\n"
        if job_name:
            message += f"🔧 *Job*: `{job_name}`\n"
        message += f"⏰ *Time*: `{current_time}`\n"
        message += f"❌ *Error*: `{error_message}`\n\n"
        
        # Add additional info if provided
        if additional_info:
            message += "📋 *Additional Info*:\n"
            for key, value in additional_info.items():
                message += f"- {key}: {value}\n"
            message += "\n"
            
        # Add stack trace (truncate if too long to fit in Telegram message)
        max_trace_length = 3500  # Telegram has ~4000 char limit for messages
        if len(stack_trace) > max_trace_length:
            stack_trace = stack_trace[:max_trace_length] + "...[truncated]"
            
        message += f"📊 *Stack Trace*:\n```\n{stack_trace}\n```"
        
        # Send to error monitoring bot
        response = requests.post(
            f"https://api.telegram.org/bot{ERROR_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": ERROR_CHAT_ID,
                "text": message,
                "parse_mode": "Markdown"
            },
            timeout=10
        )
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Failed to send error to monitoring bot: {e}")
        logger.error(traceback.format_exc())
        return False

def send_user_friendly_message(bot_token, chat_id, service_type=None):
    """
    Send a generic user-friendly error message without exposing error details.
    
    Args:
        bot_token (str): The Telegram bot token
        chat_id (str/int): The chat ID to send message to
        service_type (str, optional): Type of service for more specific messaging
    """
    try:
        # Create a generic message based on service type
        if service_type == "menores":
            message = "I'm having trouble checking appointment availability for Menores Ley 36 right now. Please try again later."
        elif service_type == "certificate":
            message = "I'm having trouble checking certificate appointment availability right now. Please try again later."
        else:
            message = "I encountered a temporary issue while processing your request. Please try again later."
        
        # Send message to user
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message},
            timeout=10
        )
    except Exception as e:
        logger.error(f"Failed to send user-friendly message: {e}")