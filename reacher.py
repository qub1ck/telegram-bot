import asyncio
import logging
import traceback
import random
import os
from datetime import datetime
from typing import List, Optional, Dict, Tuple

from playwright.async_api import async_playwright, TimeoutError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# URL constants
MENORES_URL = "https://www.exteriores.gob.es/Consulados/lahabana/es/ServiciosConsulares/Paginas/menorescita.aspx"
CERTIFICADO_URL = "https://www.exteriores.gob.es/Consulados/lahabana/es/ServiciosConsulares/Paginas/index.aspx?scco=Cuba&scd=166&scca=Certificados&scs=Certificado+de+nacimiento"

# Service option mapping
SERVICE_URL_MAP = {
    "Reservar Cita de Minores Ley 36": MENORES_URL,
    "INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO": MENORES_URL,
    "INSCRIPCIÓN MENORES LEY36 OPCIÓN 2 HIJOS": MENORES_URL,
    "INSCRIPCIÓN MENORES LEY36 OPCIÓN 3 HIJOS": MENORES_URL,
    "Solicitar certificación de Nacimiento": CERTIFICADO_URL,
    "Solicitar certificación de Nacimiento para DNI": CERTIFICADO_URL
}


class ProxyManager:
    """Manage proxy loading, selection, and rotation."""

    def __init__(self, proxy_file: str = "proxy.txt"):
        self.proxy_file = proxy_file
        self.proxies: List[Tuple[str, int]] = []
        self.used_proxies: List[Tuple[str, int]] = []
        self.failed_proxies: Dict[Tuple[str, int], int] = {}
        self.proxy_username = "vqytkifr"
        self.proxy_password = "x90e6lupyath"

    async def load_proxies(self) -> List[Tuple[str, int]]:
        """Load proxies from file with error handling."""
        try:
            with open(self.proxy_file, "r") as f:
                content = f.read()
                proxies = []
                for line in content.strip().split('\n'):
                    if ':' in line:
                        host, port = line.strip().split(':')
                        proxies.append((host, int(port)))
            logger.info(f"Loaded {len(proxies)} proxies")
            return proxies
        except FileNotFoundError:
            logger.error(f"Proxies file {self.proxy_file} not found")
            return []
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
            return []

    async def get_proxy(self) -> Optional[Dict[str, str]]:
        """Select a proxy with rotation strategy and failure tracking."""
        if not self.proxies:
            self.proxies = await self.load_proxies()
            if not self.proxies:
                logger.warning("No proxies available, proceeding without proxy")
                return None

        # Filter out proxies with too many failures
        good_proxies = [p for p in self.proxies if self.failed_proxies.get(p, 0) < 3]
        if not good_proxies:
            logger.warning("All proxies have excessive failures, no more proxies available")
            return None

        # Select a random proxy from the good ones
        proxy = random.choice(good_proxies)
        self.proxies.remove(proxy)
        self.used_proxies.append(proxy)

        return {
            "server": f"{proxy[0]}:{proxy[1]}",
            "username": self.proxy_username,
            "password": self.proxy_password
        }

    def mark_proxy_failed(self, proxy: Dict[str, str]):
        """Mark a proxy as failed to reduce its chances of being selected again."""
        if not proxy:
            return

        server = proxy.get("server", "")
        if not server or ":" not in server:
            return

        host, port_str = server.split(":")
        try:
            port = int(port_str)
            proxy_tuple = (host, port)
            self.failed_proxies[proxy_tuple] = self.failed_proxies.get(proxy_tuple, 0) + 1
            logger.info(f"Marked proxy {server} as failed (count: {self.failed_proxies[proxy_tuple]})")
        except ValueError:
            logger.error(f"Invalid port in proxy server: {server}")


async def check_appointments_async(service_option: str, preferred_date: Optional[str] = None) -> Optional[List[str]]:
    """
    Check appointment availability for a specific service option using proxy rotation.

    Args:
        service_option: The service option selected by the user
        preferred_date: Optional preferred date in format DD/MM/YYYY

    Returns:
        List of available dates or None if no dates are available
    """
    logger.info(f"Checking appointments for service: {service_option}")

    # Initialize proxy manager
    proxy_manager = ProxyManager()
    max_attempts = 5

    for attempt in range(max_attempts):
        # Get a proxy for this attempt (first attempt is without proxy)
        proxy_options = await proxy_manager.get_proxy() if attempt > 0 else None

        browser = None
        context = None

        try:
            logger.info(f"Attempt {attempt + 1}/{max_attempts} {'with proxy' if proxy_options else 'without proxy'}")

            async with async_playwright() as p:
                # Launch browser with proxy if provided
                browser_args = {"headless": True}
                if proxy_options:
                    browser_args["proxy"] = proxy_options

                # Use different user agent each time to avoid detection
                user_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
                ]

                browser = await p.chromium.launch(**browser_args)
                context = await browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    user_agent=random.choice(user_agents),
                    locale=random.choice(["es-ES", "en-US", "en-GB"]),
                    timezone_id=random.choice(["Europe/Madrid", "Europe/London", "America/Havana"])
                )

                # Set a longer timeout for all operations
                context.set_default_timeout(60000)
                page = await context.new_page()

                # Determine which URL to use based on the service option
                base_url = SERVICE_URL_MAP.get(service_option, MENORES_URL)
                logger.info(f"Navigating to {base_url}")

                # Script to handle alert dialogs automatically
                await page.evaluate("window.onbeforeunload = null;")

                # Set up dialog handler
                page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))

                # Navigate to the appropriate URL with retry mechanism
                max_navigation_retries = 3
                for nav_attempt in range(max_navigation_retries):
                    try:
                        await page.goto(base_url, wait_until="networkidle", timeout=60000)
                        break
                    except Exception as nav_e:
                        if nav_attempt == max_navigation_retries - 1:
                            raise
                        logger.warning(f"Navigation retry {nav_attempt + 1}/{max_navigation_retries}: {str(nav_e)}")
                        await asyncio.sleep(2)

                # For certificate services
                if "Solicitar certificación de Nacimiento" in service_option:
                    result = await handle_certificate_service(page, service_option, preferred_date)
                    if result is not None:
                        return result
                # For Menores Ley 36 services
                else:
                    result = await handle_menores_service(page, service_option, preferred_date)
                    if result is not None:
                        return result

                # If we get here, no results were found, try next attempt
                logger.info("No results found in this attempt, will try again")

        except TimeoutError as e:
            logger.error(f"Timeout error: {e}")
            if proxy_options:
                proxy_manager.mark_proxy_failed(proxy_options)
        except Exception as e:
            logger.error(f"Error checking appointments: {e}")
            logger.error(traceback.format_exc())
            if proxy_options:
                proxy_manager.mark_proxy_failed(proxy_options)
        finally:
            # Clean up resources
            if context:
                await context.close()
            if browser:
                await browser.close()

            # Add some random delay between attempts
            if attempt < max_attempts - 1:
                delay = random.uniform(1.0, 5.0)
                await asyncio.sleep(delay)

    # If we've exhausted all attempts
    logger.error(f"Failed to check appointments after {max_attempts} attempts")
    return None


async def handle_certificate_service(page, service_option, preferred_date):
    """Handle the navigation flow for certificate services."""
    logger.info("Certificate service detected")

    # Click on the service link
    if "para DNI" in service_option:
        logger.info("Looking for DNI certificate link")
        cert_link = await page.query_selector("text=Solicitar certificación de Nacimiento para DNI")
    else:
        logger.info("Looking for regular certificate link")
        cert_link = await page.query_selector("text=Solicitar certificación de Nacimiento")

    if cert_link:
        await cert_link.click()
        logger.info("Clicked certificate link")
    else:
        logger.error("Certificate link not found, trying alternative method")
        # Try clicking anything that looks like a relevant link
        links = await page.query_selector_all("a")
        for link in links:
            text = await link.text_content()
            if "certificación" in text.lower() and "nacimiento" in text.lower():
                await link.click()
                logger.info(f"Clicked alternative certificate link: {text}")
                break

    # Wait for the page to load after clicking
    await page.wait_for_load_state("networkidle", timeout=60000)

    # Click on "Continue" button
    continue_button = await page.query_selector("#bktContinue")
    if continue_button:
        await continue_button.click()
        logger.info("Clicked continue button for certificate")
    else:
        logger.error("Continue button not found for certificate")

    # Check for available dates
    await page.wait_for_load_state("networkidle", timeout=60000)

    # Check if "No hay horas" message is present
    no_dates = await page.query_selector("text=No hay horas")
    if no_dates:
        logger.info("No available dates found for certificate")
        return []

    # Extract available dates
    available_dates = await extract_dates(page, preferred_date)
    return available_dates


async def handle_menores_service(page, service_option, preferred_date):
    """Handle the navigation flow for Menores Ley 36 services."""
    logger.info("Menores Ley 36 service detected")

    # Click on the service link
    menores_link = await page.query_selector("text=Reservar Cita de Menores Ley 36")
    if menores_link:
        await menores_link.click()
        logger.info("Clicked Menores Ley 36 link")
    else:
        logger.error("Menores Ley 36 link not found, trying alternative method")
        # Try clicking anything that looks like a relevant link
        links = await page.query_selector_all("a")
        for link in links:
            text = await link.text_content()
            if "menores" in text.lower() and "ley 36" in text.lower():
                await link.click()
                logger.info(f"Clicked alternative Menores link: {text}")
                break

    # Wait for the page to load after clicking
    await page.wait_for_load_state("networkidle", timeout=60000)

    # Wait for and click the captcha button (may be required)
    captcha_button = await page.query_selector("#idCaptchaButton")
    if captcha_button:
        await captcha_button.click()
        logger.info("Clicked captcha button")

    # Accept terms with continue button
    continue_button = await page.query_selector("#bktContinue")
    if continue_button:
        await continue_button.click()
        logger.info("Clicked continue button for Menores")
    else:
        logger.error("Continue button not found for Menores")

    # Accept terms with acceptar button (may be needed)
    acceptar_button = await page.query_selector("text=Aceptar")
    if acceptar_button:
        await acceptar_button.click()
        logger.info("Clicked acceptar button")

    # Wait for service options to load
    try:
        await page.wait_for_selector("#idListServices", timeout=60000)
        logger.info("Service options loaded")
    except TimeoutError:
        logger.error("Service options not loaded, trying to continue anyway")

    # Find and click the specific option based on user choice
    option_text = service_option
    if service_option == "Reservar Cita de Minores Ley 36":
        # If user just selected the general option, default to first option
        option_text = "INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO"

    logger.info(f"Looking for option: {option_text}")

    # Try various selector strategies to find the option
    selectors_to_try = [
        f"text={option_text}",
        f"//div[contains(text(), '{option_text}')]",
        f"//a[contains(text(), '{option_text}')]"
    ]

    option_found = False
    for selector in selectors_to_try:
        try:
            option_link = await page.query_selector(selector)
            if option_link:
                await option_link.click()
                logger.info(f"Clicked on option using selector: {selector}")
                option_found = True
                break
        except Exception as e:
            logger.warning(f"Error using selector {selector}: {e}")

    if not option_found:
        logger.error(f"Option not found with standard selectors: {option_text}")
        # Try alternative approach - look at all service elements
        services = await page.query_selector_all(".clsBktServiceName")
        for service in services:
            text = await service.text_content()
            if option_text.lower() in text.lower():
                await service.click()
                logger.info(f"Clicked alternative service: {text}")
                option_found = True
                break

    if not option_found:
        logger.error("Could not find and click the requested service option")
        return None

    # Wait for the calendar to load
    await page.wait_for_load_state("networkidle", timeout=60000)

    # Check if "No hay horas" message is present
    no_dates = await page.query_selector("text=No hay horas")
    if no_dates:
        logger.info(f"No available dates found for {option_text}")
        return []

    # Extract available dates
    available_dates = await extract_dates(page, preferred_date)
    return available_dates


async def extract_dates(page, preferred_date: Optional[str] = None) -> List[str]:
    """
    Extract available dates from the appointment page.

    Args:
        page: Playwright page object
        preferred_date: Optional preferred date in format DD/MM/YYYY

    Returns:
        List of available dates
    """
    try:
        # First try to find specifically marked available dates
        dates = await page.evaluate('''
            () => {
                const availableDates = [];
                // Look for available dates with various selectors
                const selectors = [
                    '.available-date', 
                    '[data-available="true"]',
                    '.calendar-day-available',
                    '.ui-state-active',
                    '.ui-state-default:not(.ui-state-disabled)'
                ];

                for (const selector of selectors) {
                    document.querySelectorAll(selector).forEach(el => {
                        const dateText = el.innerText.trim();
                        if (dateText) {
                            availableDates.push(dateText);
                        }
                    });
                }

                return availableDates;
            }
        ''')

        if not dates or len(dates) == 0:
            # If no dates found with first method, try alternative method
            dates = await page.evaluate('''
                () => {
                    // Try to find dates in the calendar
                    const calendarDays = Array.from(document.querySelectorAll('[data-handler="selectDay"]'));
                    if (calendarDays.length > 0) {
                        return calendarDays
                            .filter(day => !day.classList.contains('ui-state-disabled'))
                            .map(day => {
                                const dayNum = day.innerText.trim();
                                const month = document.querySelector('.ui-datepicker-month')?.innerText.trim() || '';
                                const year = document.querySelector('.ui-datepicker-year')?.innerText.trim() || '';
                                return `${dayNum} de ${month} de ${year}`;
                            });
                    }

                    // Try another calendar format
                    const availableDays = Array.from(document.querySelectorAll('.day:not(.disabled)'));
                    if (availableDays.length > 0) {
                        const currentMonth = document.querySelector('.datepicker-switch')?.innerText.trim() || '';
                        return availableDays.map(day => `${day.innerText.trim()} ${currentMonth}`);
                    }

                    return [];
                }
            ''')

        if dates and len(dates) > 0:
            logger.info(f"Found {len(dates)} available dates")

            # If preferred date is specified, check if it's available
            if preferred_date:
                try:
                    preferred_dt = datetime.strptime(preferred_date, "%d/%m/%Y")

                    # Find exact match or closest date
                    exact_match = None
                    closest_date = None
                    min_diff = float('inf')

                    for date_str in dates:
                        # Try different date formats
                        date_formats = [
                            "%d de %B de %Y",
                            "%d/%m/%Y",
                            "%d-%m-%Y",
                            "%Y-%m-%d",
                            "%d %B %Y"
                        ]

                        for fmt in date_formats:
                            try:
                                date_dt = datetime.strptime(date_str, fmt)
                                if date_dt.date() == preferred_dt.date():
                                    # Exact match found
                                    return [f"SELECTED: {date_str}"]

                                # Calculate difference in days
                                diff = abs((date_dt.date() - preferred_dt.date()).days)
                                if diff < min_diff:
                                    min_diff = diff
                                    closest_date = date_str
                                break
                            except ValueError:
                                continue

                    if closest_date:
                        return [f"CLOSEST AVAILABLE: {closest_date}"] + [d for d in dates if d != closest_date]

                except ValueError:
                    logger.error(f"Invalid preferred date format: {preferred_date}")

            return dates
        else:
            logger.info("No available dates found")
            return []

    except Exception as e:
        logger.error(f"Error extracting dates: {e}")
        logger.error(traceback.format_exc())
        return []