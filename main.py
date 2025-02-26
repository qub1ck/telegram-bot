import logging
import os
import subprocess
import traceback
from flask import Flask, request, jsonify
from telegram import Update, ReplyKeyboardMarkup, Message, Chat, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext, CallbackQueryHandler
from bot_users import (
    upsert_user, add_user_job, remove_user_job, get_user_jobs,
    initialize_db, get_all_active_jobs, is_job_ready_to_search
)
from reacher import check_appointments_async
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask for webhook
flask_app = Flask(__name__)


# Get token from environment variables or token.txt file
def get_token():
    """Retrieve the Telegram bot token from environment variables or a file."""
    try:
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        if token:
            return token
    except Exception as e:
        logger.error(f"Error retrieving token: {str(e)}")
    return None


# Get GitHub Pages URL from environment variables or use a default
GITHUB_PAGES_URL = os.environ.get("GITHUB_PAGES_URL", "https://qub1ck.github.io/telegram-bot")

# This will be set during initialization
telegram_app = None


@flask_app.route("/start-search", methods=["POST"])
def start_search():
    """Start a search after form submission."""
    try:
        # Log raw incoming data for debugging
        logger.error(f"Start Search Request Received - Raw Data: {request.json}")

        # Get data from JSON request
        data = request.json
        user_id = data.get("user_id")
        job_name = data.get("job_name")

        # Validate input
        if not user_id or not job_name:
            logger.error(f"Missing required parameters: user_id={user_id}, job_name={job_name}")
            return jsonify({"status": "error", "message": "Missing user_id or job_name"}), 400

        # Ensure user_id is an integer
        user_id = int(user_id)

        logger.error(f"Processing start search for user {user_id}, job {job_name}")

        # Schedule the async function to run
        async def start_search_task():
            try:
                # Extensive logging for job readiness check
                logger.error(f"Checking job readiness for user {user_id}, job {job_name}")
                job_ready = await is_job_ready_to_search(user_id, job_name)

                logger.error(f"Job {job_name} ready status: {job_ready}")

                if not job_ready:
                    logger.error(f"Job {job_name} for user {user_id} is not marked as active")
                    return jsonify({"status": "error", "message": "Job not ready"}), 400

                # Extract the original option from the job name
                # e.g., "Maria, 1 HIJO" -> "INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO"
                option_part = job_name.split(", ")[-1]
                original_option = f"INSCRIPCIÓN MENORES LEY36 OPCIÓN {option_part}"

                logger.error(f"Original option for {job_name}: {original_option}")

                # Create a fake update to pass to show_options
                fake_update = Update(update_id=0,
                                     message=Message(message_id=0,
                                                     chat=Chat(id=user_id, type='private'),
                                                     date=None))

                # Start the background job
                job_name_to_run = f"check_dates_{user_id}_{job_name}"

                # Check if the job already exists and remove it
                existing_jobs = telegram_app.job_queue.get_jobs_by_name(job_name_to_run)
                if existing_jobs:
                    for job in existing_jobs:
                        job.schedule_removal()
                    logger.error(f"Removed {len(existing_jobs)} existing jobs for {job_name_to_run}")

                # Start new job
                telegram_app.job_queue.run_repeating(
                    check_dates_continuously,
                    interval=30,
                    first=0,
                    data={'chat_id': user_id, 'user_choice': original_option, 'user_id': user_id, 'job_name': job_name},
                    name=job_name_to_run,
                    job_kwargs={'max_instances': 2}
                )
                logger.error(f"Started background job {job_name_to_run}")

                # Send confirmation message about search starting
                try:
                    await telegram_app.bot.send_message(
                        chat_id=int(user_id),
                        text=f"Starting automatic search for {job_name}. I'll notify you when appointments become available.",
                        reply_markup=await show_options(fake_update, None)
                    )
                    logger.error(f"Sent confirmation message to user {user_id}")
                except Exception as e:
                    logger.error(f"Error sending Telegram message: {str(e)}")
                    logger.error(f"Traceback: {traceback.format_exc()}")

                return jsonify({"status": "success", "message": "Search job scheduled"})

            except Exception as e:
                logger.error(f"Error in start_search_task: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                return jsonify({"status": "error", "message": str(e)}), 500

        # Schedule the task to run
        try:
            # We need to get the application instance and create the task
            app_instance = Application.get_instance()
            if app_instance:
                app_instance.create_task(start_search_task())
                logger.error(f"Scheduled start_search_task for user {user_id}, job {job_name}")
                return jsonify({"status": "success", "message": "Search job scheduled"})
            else:
                logger.error("Application.get_instance() returned None")
                return jsonify({"status": "error", "message": "Could not schedule search task"}), 500
        except Exception as e:
            logger.error(f"Error scheduling start_search_task: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return jsonify({"status": "error", "message": f"Error scheduling search: {str(e)}"}), 500

    except Exception as e:
        logger.error(f"General error in start_search: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": "Failed to process search request"}), 500


async def show_options(update: Update, context: CallbackContext):
    """Show the main options menu to the user, conditionally displaying the 'Cancel search' and 'Check my appointments' buttons."""
    if update.message:
        user_id = update.message.from_user.id
    elif update.callback_query:
        user_id = update.callback_query.from_user.id
    else:
        logger.error("No message or callback_query found in update.")
        return None

    keyboard = [
        ['Search for new appointments'],
    ]  # Default options

    user_jobs = await get_user_jobs(user_id)
    if user_jobs:
        # If the user has active jobs, show the "Cancel search" and "Check my appointments" buttons
        keyboard.append(['Cancel search for appointment'])
        keyboard.append(['Check my appointments'])

    return ReplyKeyboardMarkup(keyboard, one_time_keyboard=False, resize_keyboard=True)


async def start(update: Update, context: CallbackContext):
    """Handle the /start command."""
    await initialize_db()
    user_id = update.message.from_user.id
    await upsert_user(user_id)
    await update.message.reply_text("Hello! I'm your appointment bot 🤖!")
    await update.message.reply_text("I can help you search for appointments and provide registration forms.",
                                    reply_markup=await show_options(update, context))


async def send_registration_forms(update: Update, context: CallbackContext):
    """Send a message with registration form links."""
    chat_id = update.message.chat_id  # Get the user's chat ID

    # If there's a pending job name, use it in the URLs
    job_name = ""
    if 'pending_job_name' in context.user_data:
        job_name = f"&job_name={context.user_data['pending_job_name']}"

    # Include the chat_id in the form URLs
    form_urls_with_chat_id = {
        "option1": f"{GITHUB_PAGES_URL}/first_option.html?chat_id={chat_id}{job_name}",
        "option2": f"{GITHUB_PAGES_URL}/second_option.html?chat_id={chat_id}{job_name}",
        "option3": f"{GITHUB_PAGES_URL}/third_option.html?chat_id={chat_id}{job_name}",
    }

    keyboard = [
        [InlineKeyboardButton("Registration for 1 Child", url=form_urls_with_chat_id["option1"])],
        [InlineKeyboardButton("Registration for 2 Children", url=form_urls_with_chat_id["option2"])],
        [InlineKeyboardButton("Registration for 3 Children", url=form_urls_with_chat_id["option3"])],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Please select the registration form you need:",
        reply_markup=reply_markup,
    )

    # Show main options again
    await update.message.reply_text("You can also choose from these options:",
                                    reply_markup=await show_options(update, context))


async def handle_option(update: Update, context: CallbackContext):
    """Handle user-selected options."""
    user_id = update.message.from_user.id
    user_choice = update.message.text

    # Registration forms option
    if user_choice == "Registration forms":
        await send_registration_forms(update, context)
        return

    if user_choice == "Cancel search for appointment":
        user_jobs = await get_user_jobs(user_id)
        if not user_jobs:
            await update.message.reply_text("No active searches to cancel.",
                                            reply_markup=await show_options(update, context))
            return

        # Create an inline keyboard for the user to select which job to cancel
        keyboard = [[InlineKeyboardButton(job, callback_data=f"cancel_{job}")] for job in user_jobs]
        keyboard.append(
            [InlineKeyboardButton("Cancel all appointments", callback_data="cancel_all")])  # Add "Cancel all" option
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Select the appointment to cancel:", reply_markup=reply_markup)
        return

    if user_choice == "Check my appointments":
        user_jobs = await get_user_jobs(user_id)
        if not user_jobs:
            await update.message.reply_text("No active searches to check.",
                                            reply_markup=await show_options(update, context))
            return

        # Create an inline keyboard for the user to select which job to check
        keyboard = [
            [InlineKeyboardButton(job, callback_data=f"check_{job}")] for job in user_jobs
        ]
        keyboard.append([InlineKeyboardButton("Check all appointments", callback_data="check_all")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Select an appointment to check:", reply_markup=reply_markup)
        return

    if user_choice == "Search for new appointments":
        # Show the appointment options
        options_keyboard = [
            ['INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO'],
            ['INSCRIPCIÓN MENORES LEY36 OPCIÓN 2 HIJOS'],
            ['INSCRIPCIÓN MENORES LEY36 OPCIÓN 3 HIJOS'],
            ['CANCEL']
        ]
        reply_markup = ReplyKeyboardMarkup(options_keyboard, one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Please choose one of the following options:", reply_markup=reply_markup)
        return

    if user_choice in ["INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO",
                       "INSCRIPCIÓN MENORES LEY36 OPCIÓN 2 HIJOS",
                       "INSCRIPCIÓN MENORES LEY36 OPCIÓN 3 HIJOS"]:
        # Ask for the name of the appointment
        await update.message.reply_text("Please provide a name for this appointment (e.g., 'John' or 'Maria'):")
        context.user_data['pending_job'] = user_choice  # Store the selected option temporarily

        # Also store the option number for form link
        if "1 HIJO" in user_choice:
            context.user_data['form_option'] = "first"
        elif "2 HIJOS" in user_choice:
            context.user_data['form_option'] = "second"
        elif "3 HIJOS" in user_choice:
            context.user_data['form_option'] = "third"
        return

    if 'pending_job' in context.user_data:
        # User has provided a name for the appointment
        user_provided_name = update.message.text
        selected_option = context.user_data['pending_job']  # Retrieve the original option text
        form_option = context.user_data.get('form_option')  # Get the form option

        # Format the job name as "Name, Selected Option"
        job_name = f"{user_provided_name}, {selected_option.split()[-2]} {selected_option.split()[-1]}"

        # Store this for the registration form
        context.user_data['pending_job_name'] = job_name

        # Check if the name is already in use (case-insensitive)
        user_jobs = await get_user_jobs(user_id)
        if any(job.lower() == job_name.lower() for job in user_jobs):
            await update.message.reply_text(
                f"The name '{user_provided_name}' is already in use. Please choose another name.")
            # Stay in the "pending job" state to wait for another name
            return

        if len(user_jobs) >= 15:
            await update.message.reply_text("You have reached the maximum number of active searches (15).",
                                            reply_markup=await show_options(update, context))
            return

        # Add the job as pending_form (will be updated to active after form submission)
        job_added = await add_user_job(user_id, job_name)
        if not job_added:
            await update.message.reply_text("Failed to create job. Please try again.",
                                            reply_markup=await show_options(update, context))
            return

        await update.message.reply_text(
            f"Name '{job_name}' accepted.")

        # Send registration form link
        if form_option:
            chat_id = update.message.chat_id
            form_url = f"{GITHUB_PAGES_URL}/{form_option}_option.html?chat_id={chat_id}&job_name={job_name}"
            keyboard = [[InlineKeyboardButton("Fill Registration Form", url=form_url)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(f"Please fill out the registration form to start searching for appointments:",
                reply_markup=reply_markup
            )

        # Inform user that search will start after form submission
        await update.message.reply_text(
            "Your appointment search will begin automatically after you submit the registration form.",
            reply_markup=await show_options(update, context)
        )

        # Clear the pending job state after successfully adding the job
        del context.user_data['pending_job']
        if 'form_option' in context.user_data:
            del context.user_data['form_option']
        return

    if user_choice == "CANCEL":
        await update.message.reply_text("Returning to main menu", reply_markup=await show_options(update, context))
        return

    else:
        await update.message.reply_text(
            "Sorry, I don't understand that option. Please select from the available choices.",
            reply_markup=await show_options(update, context))


async def handle_cancel_job(update: Update, context: CallbackContext):
    """Handle the callback query for canceling a job."""
    query = update.callback_query
    await query.answer()  # Acknowledge the callback query

    user_id = query.from_user.id
    callback_data = query.data

    if callback_data == "cancel_all":
        # Cancel all appointments for the user
        user_jobs = await get_user_jobs(user_id)
        for job in user_jobs:
            await remove_user_job(user_id, job)
            # Remove the background job
            job_name_to_cancel = f"check_dates_{user_id}_{job}"
            existing_jobs = context.job_queue.get_jobs_by_name(job_name_to_cancel)
            if existing_jobs:
                for job in existing_jobs:
                    job.schedule_removal()
        await query.edit_message_text("All appointments have been canceled.")
    else:
        # Cancel a specific appointment
        job_name = callback_data.replace("cancel_", "")
        await remove_user_job(user_id, job_name)
        await query.edit_message_text(f"Search for {job_name} has been canceled.")

        # Remove the background job
        job_name_to_cancel = f"check_dates_{user_id}_{job_name}"
        existing_jobs = context.job_queue.get_jobs_by_name(job_name_to_cancel)
        if existing_jobs:
            for job in existing_jobs:
                job.schedule_removal()

    # Create a fake Update object with a valid Message and User
    fake_message = Message(
        message_id=0,
        date=None,
        chat=Chat(id=query.message.chat_id, type='private'),
        from_user=query.from_user  # Include the from_user attribute
    )
    fake_update = Update(update_id=0, message=fake_message)

    # Show options after canceling the job(s)
    await query.message.reply_text("Please choose an option:", reply_markup=await show_options(fake_update, context))


async def check_dates_continuously(context: CallbackContext):
    """Check for available dates in the background."""
    job_data = context.job.data
    chat_id = job_data['chat_id']
    user_choice = job_data['user_choice']  # Original option text (e.g., "INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO")
    user_id = job_data['user_id']
    job_name = job_data['job_name']  # Formatted job name (e.g., "Maria, 1 HIJO")

    logger.info(f"Running background job for user {chat_id} with choice {user_choice}")

    try:
        # Verify job is still active
        job_ready = await is_job_ready_to_search(user_id, job_name)
        if not job_ready:
            logger.info(f"Job {job_name} for user {user_id} is no longer active, stopping search")
            context.job.schedule_removal()
            return

        available_dates = await check_appointments_async(user_choice)  # Pass the original option text
        if available_dates:
            await context.bot.send_message(chat_id,
                                           f"Available dates found for {job_name}: {', '.join(available_dates)}")
            logger.info(f"Available dates found for user {chat_id}")
            context.job.schedule_removal()
            await remove_user_job(user_id, job_name)
            # Create a fake Update object to pass to show_options
            fake_update = Update(update_id=0,
                                 message=Message(message_id=0, chat=Chat(id=chat_id, type='private'), date=None))
            await context.bot.send_message(
                chat_id=chat_id,
                text="Please choose an option:",
                reply_markup=await show_options(fake_update, context)
            )
        else:
            logger.info(f"No available dates found for user {chat_id}")
    except Exception as e:
        logger.error(f"Error in check_dates_continuously for user {chat_id}: {e}")
        await remove_user_job(user_id, job_name)
        await context.bot.send_message(chat_id, "The service is currently unavailable. Please try again later.")
        # Create a fake Update object to pass to show_options
        fake_update = Update(update_id=0,
                             message=Message(message_id=0, chat=Chat(id=chat_id, type='private'), date=None))
        await context.bot.send_message(
            chat_id=chat_id,
            text="Please choose an option:",
            reply_markup=await show_options(fake_update, context)
        )
        context.job.schedule_removal()


async def handle_check_appointments(update: Update, context: CallbackContext):
    """Handle the callback query for checking appointments."""
    query = update.callback_query
    await query.answer()  # Acknowledge the callback query

    user_id = query.from_user.id
    callback_data = query.data

    if callback_data == "check_all":
        # Check all appointments
        user_jobs = await get_user_jobs(user_id)
        for job in user_jobs:
            # Extract the original option text from the job name
            original_option = job.split(", ")[-1]  # e.g., "1 HIJO" -> "INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO"
            original_option_text = f"INSCRIPCIÓN MENORES LEY36 OPCIÓN {original_option}"
            available_dates = await check_appointments_async(original_option_text)
            if available_dates:
                await query.message.reply_text(f"Available dates found for {job}: {', '.join(available_dates)}")
            else:
                await query.message.reply_text(f"No available dates found for {job}.")
    else:
        # Check a specific appointment
        job_name = callback_data.replace("check_", "")
        # Extract the original option text from the job name
        original_option = job_name.split(", ")[-1]  # e.g., "1 HIJO" -> "INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO"
        original_option_text = f"INSCRIPCIÓN MENORES LEY36 OPCIÓN {original_option}"
        available_dates = await check_appointments_async(original_option_text)
        if available_dates:
            await query.message.reply_text(f"Available dates found for {job_name}: {', '.join(available_dates)}")
        else:
            await query.message.reply_text(f"No available dates found for {job_name}.")

    # Return to the main menu
    await query.message.reply_text("Please choose an option:", reply_markup=await show_options(update, context))


async def restart_active_jobs(app: Application):
    """Restart all active jobs on bot startup."""
    active_jobs = await get_all_active_jobs()
    logger.info(f"Restarting {len(active_jobs)} active jobs.")
    for job in active_jobs:
        user_id = job["user_id"]
        job_name = job["job_name"]
        original_option = job_name.split(", ")[-1]
        original_option_text = f"INSCRIPCIÓN MENORES LEY36 OPCIÓN {original_option}"

        logger.info(f"Restarting job for user {user_id} with choice {job_name}")

        job_name_to_run = f"check_dates_{user_id}_{job_name}"
        app.job_queue.run_repeating(
            check_dates_continuously,
            interval=30,
            first=5,
            data={'chat_id': user_id, 'user_choice': original_option_text, 'user_id': user_id, 'job_name': job_name},
            name=job_name_to_run,
            job_kwargs={'max_instances': 2}
        )


async def check_for_new_jobs(context: CallbackContext):
    """Periodically check for new active jobs that need to be started."""
    try:
        logger.info("Checking for new active jobs...")
        active_jobs = await get_all_active_jobs()

        for job in active_jobs:
            user_id = job["user_id"]
            job_name = job["job_name"]

            # Check if this job is already running
            job_name_to_run = f"check_dates_{user_id}_{job_name}"
            existing_jobs = context.job_queue.get_jobs_by_name(job_name_to_run)

            if not existing_jobs:
                # This job is active but not running yet, start it
                logger.info(f"Found new active job not yet running: {job_name} for user {user_id}")

                # Extract the original option from the job name
                option_part = job_name.split(", ")[-1]
                original_option = f"INSCRIPCIÓN MENORES LEY36 OPCIÓN {option_part}"

                # Start the background job
                context.job_queue.run_repeating(
                    check_dates_continuously,
                    interval=30,
                    first=5,
                    data={'chat_id': user_id, 'user_choice': original_option, 'user_id': user_id, 'job_name': job_name},
                    name=job_name_to_run,
                    job_kwargs={'max_instances': 2}
                )
                logger.info(f"Started new background job {job_name_to_run}")
    except Exception as e:
        logger.error(f"Error in check_for_new_jobs: {str(e)}")


async def on_startup(app: Application):
    """Tasks to run after the bot starts."""
    logger.info("Bot startup process beginning...")

    try:
        await initialize_db()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Database initialization error: {str(e)}")

    try:
        await restart_active_jobs(app)
        logger.info("Active jobs restarted successfully.")
    except Exception as e:
        logger.error(f"Error restarting active jobs: {str(e)}")

    # Add a job to check for new active jobs periodically
    app.job_queue.run_repeating(
        check_for_new_jobs,
        interval=30,  # Check every 30 seconds
        first=5,  # Start checking after 5 seconds
        name="check_for_new_jobs",
        job_kwargs={'max_instances': 2}
    )
    logger.info("Added job checker to periodically check for new active jobs")

    # Store reference to the telegram app globally
    global telegram_app
    telegram_app = app
    logger.info("Telegram app global variable set.")


def main():
    """Run the Telegram bot."""
    token = get_token()
    if not token:
        logger.error(
            "No Telegram bot token found. Please set the TELEGRAM_BOT_TOKEN environment variable.")
        return

    try:

        subprocess.run(["playwright", "install"], check=True)
        app = Application.builder().token(token).post_init(on_startup).build()

        # Add handlers
        app.add_handler(CommandHandler("start", start))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_option))
        app.add_handler(CallbackQueryHandler(handle_cancel_job, pattern="^cancel_"))
        app.add_handler(CallbackQueryHandler(handle_check_appointments, pattern="^check_"))

        logger.info("Bot handlers added. Starting bot...")

        # Run the Flask app in a separate thread
        from threading import Thread
        thread = Thread(target=lambda: flask_app.run(host="0.0.0.0", port=5001, debug=False))
        thread.daemon = True
        thread.start()
        logger.info("Flask app started in separate thread.")

        # Run the Telegram bot
        app.run_polling()

    except Exception as e:
        logger.error(f"Critical error in main(): {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    main()
