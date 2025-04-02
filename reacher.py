import asyncio
import logging
import traceback
import random
import os
import subprocess
import time
from datetime import datetime
from typing import List, Optional, Dict
from playwright.async_api import async_playwright, TimeoutError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MENORES_URL = "https://www.exteriores.gob.es/Consulados/lahabana/es/ServiciosConsulares/Paginas/menorescita.aspx"
CERTIFICADO_URL = "https://www.exteriores.gob.es/Consulados/lahabana/es/ServiciosConsulares/Paginas/index.aspx?scco=Cuba&scd=166&scca=Certificados&scs=Certificado+de+nacimiento"

SERVICE_URL_MAP = {
    "Reservar Cita de Minores Ley 36": MENORES_URL,
    "INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO": MENORES_URL,
    "INSCRIPCIÓN MENORES LEY36 OPCIÓN 2 HIJOS": MENORES_URL,
    "INSCRIPCIÓN MENORES LEY36 OPCIÓN 3 HIJOS": MENORES_URL,
    "Solicitar certificación de Nacimiento": CERTIFICADO_URL,
    "Solicitar certificación de Nacimiento para DNI": CERTIFICADO_URL
}


class TorManager:
    def __init__(self):
        self.tor_process = None
        self.socks_port = 9050
        self.control_port = 9051
        self.tor_password = "appointment_checker"
        self._setup_done = False
        self.tor_data_dir = os.path.join(os.getcwd(), "tor_data")
    
    async def setup(self):
        if self._setup_done:
            return True
            
        try:
            # Create data directory
            os.makedirs(self.tor_data_dir, exist_ok=True)
            
            tor_installed = subprocess.run(["which", "tor"], capture_output=True).returncode == 0
            
            if not tor_installed:
                logger.error("Tor is not installed. Please install Tor: 'apt-get install tor' or equivalent")
                return False
                
            # Create a simple torrc config file
            torrc_path = os.path.join(os.getcwd(), "temp_torrc")
            with open(torrc_path, "w") as f:
                f.write(f"""
                SocksPort {self.socks_port}
                ControlPort {self.control_port}
                HashedControlPassword 16:872860B76453A77D60CA2BB8C1A7042072093276A3D701AD684053EC4C
                DataDirectory {self.tor_data_dir}
                """)
            
            logger.info("Starting Tor...")
            self.tor_process = subprocess.Popen(
                ["tor", "-f", torrc_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for Tor to start up
            await self._wait_for_tor_startup()
            
            self._setup_done = True
            logger.info("Tor setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up Tor: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _wait_for_tor_startup(self):
        max_wait = 60
        wait_interval = 1
        
        for _ in range(max_wait):
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', self.socks_port)
                writer.close()
                await writer.wait_closed()
                logger.info("Tor is ready")
                return True
            except:
                await asyncio.sleep(wait_interval)
        
        logger.error(f"Tor didn't start properly after {max_wait} seconds")
        return False
    
    async def new_identity(self):
        try:
            try:
                import stem
                from stem import Signal
                from stem.control import Controller
            except ImportError:
                logger.warning("Stem library not installed. Attempting to install...")
                subprocess.check_call(["pip", "install", "stem"])
                import stem
                from stem import Signal
                from stem.control import Controller
                
            with Controller.from_port(port=self.control_port) as controller:
                controller.authenticate(password=self.tor_password)
                controller.signal(Signal.NEWNYM)
                logger.info("New Tor identity requested")
                
            await asyncio.sleep(2)
            return True
        except Exception as e:
            logger.error(f"Error getting new Tor identity: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def stop(self):
        if self.tor_process:
            logger.info("Stopping Tor...")
            self.tor_process.terminate()
            try:
                self.tor_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.tor_process.kill()
            
            self.tor_process = None
            self._setup_done = False


class TorProxyManager:
    def __init__(self):
        self.tor_manager = TorManager()
        self.tor_ready = False
        
    async def initialize(self):
        self.tor_ready = await self.tor_manager.setup()
        if not self.tor_ready:
            logger.error("Failed to initialize Tor. Appointments cannot be checked.")
        return self.tor_ready

    async def get_proxy(self) -> Optional[Dict[str, str]]:
        if not self.tor_ready:
            if not await self.initialize():
                return None
                
        # Request a new Tor identity
        await self.tor_manager.new_identity()
        
        return {
            "server": "socks5://127.0.0.1:9050"
        }
    
    async def cleanup(self):
        await self.tor_manager.stop()


async def check_appointments_async(service_option: str, preferred_date: Optional[str] = None, max_attempts: int = 5) -> Optional[List[str]]:
    try:
        return await asyncio.wait_for(_check_appointments_impl(service_option, preferred_date, max_attempts), timeout=180)
    except asyncio.TimeoutError:
        logger.warning(f"Complete appointment check timed out for {service_option}")
        return None
    except Exception as e:
        logger.error(f"Global error in appointment check: {e}")
        logger.error(traceback.format_exc())
        return None


async def _check_appointments_impl(service_option: str, preferred_date: Optional[str] = None, max_attempts: int = 5) -> Optional[List[str]]:
    logger.info(f"Checking appointments for service: {service_option}")

    # Initialize the Tor proxy manager
    proxy_manager = TorProxyManager()
    if not await proxy_manager.initialize():
        logger.error("Could not initialize Tor. Appointment checking aborted.")
        return None
    
    current_attempt = 0

    try:
        while current_attempt < max_attempts:
            # Always use Tor proxy
            proxy_options = await proxy_manager.get_proxy()
            if not proxy_options:
                logger.error("Failed to get Tor proxy. Skipping attempt.")
                current_attempt += 1
                continue

            browser = None
            context = None

            try:
                logger.info(f"Attempt {current_attempt + 1}/{max_attempts} with Tor")

                async with async_playwright() as p:
                    browser_args = {
                        "headless": True,
                        "proxy": proxy_options
                    }

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

                    context.set_default_timeout(60000)
                    page = await context.new_page()

                    base_url = SERVICE_URL_MAP.get(service_option, MENORES_URL)
                    logger.info(f"Navigating to {base_url}")

                    await page.evaluate("window.onbeforeunload = null;")

                    # Handle alerts/dialogs automatically
                    page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))

                    # Navigation with retry logic
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

                    # Handle service-specific flows
                    if "Solicitar certificación de Nacimiento" in service_option:
                        result = await handle_certificate_service(page, service_option, preferred_date)
                        if result is not None:
                            return result
                    else:
                        result = await handle_menores_service(page, service_option, preferred_date)
                        if result is not None:
                            return result

                    logger.info("No results found in this attempt, will try again")

            except TimeoutError as e:
                logger.error(f"Timeout error: {e}")
            except Exception as e:
                logger.error(f"Error checking appointments: {e}")
                logger.error(traceback.format_exc())
            finally:
                # Clean up resources
                try:
                    if context:
                        await context.close()
                except Exception as cleanup_error:
                    logger.warning(f"Error during context cleanup: {cleanup_error}")

                try:
                    if browser:
                        await browser.close()
                except Exception as cleanup_error:
                    logger.warning(f"Error during browser cleanup: {cleanup_error}")

                # Add delay between attempts
                if current_attempt < max_attempts - 1:
                    delay = random.uniform(1.0, 5.0)
                    await asyncio.sleep(delay)

                current_attempt += 1
    finally:
        # Always clean up Tor resources
        await proxy_manager.cleanup()

    logger.error(f"Failed to check appointments after {current_attempt} attempts")
    return None


async def handle_certificate_service(page, service_option, preferred_date):
    logger.info("Certificate service detected")

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
        links = await page.query_selector_all("a")
        for link in links:
            text = await link.text_content()
            if "certificación" in text.lower() and "nacimiento" in text.lower():
                await link.click()
                logger.info(f"Clicked alternative certificate link: {text}")
                break

    await page.wait_for_load_state("networkidle", timeout=60000)

    continue_button = await page.query_selector("#bktContinue")
    if continue_button:
        await continue_button.click()
        logger.info("Clicked continue button for certificate")
    else:
        logger.error("Continue button not found for certificate")

    await page.wait_for_load_state("networkidle", timeout=60000)

    no_dates_selectors = [
        "text=No hay horas",
        "text=No hay horas disponibles",
        "text=Inténtelo de nuevo dentro de unos días",
        ".no-appointments-message",
        "#bktNoSlot"
    ]
    
    for selector in no_dates_selectors:
        no_dates = await page.query_selector(selector)
        if no_dates:
            logger.info(f"No available dates found for certificate (matched: {selector})")
            return []

    available_dates = await extract_dates(page, preferred_date)
    return available_dates


async def handle_menores_service(page, service_option, preferred_date):
    logger.info("Menores Ley 36 service detected")

    menores_link = await page.query_selector("text=Reservar Cita de Menores Ley 36")
    if menores_link:
        await menores_link.click()
        logger.info("Clicked Menores Ley 36 link")
    else:
        logger.error("Menores Ley 36 link not found, trying alternative method")
        links = await page.query_selector_all("a")
        for link in links:
            text = await link.text_content()
            if "menores" in text.lower() and "ley 36" in text.lower():
                await link.click()
                logger.info(f"Clicked alternative Menores link: {text}")
                break

    await page.wait_for_load_state("networkidle", timeout=60000)

    captcha_button = await page.query_selector("#idCaptchaButton")
    if captcha_button:
        await captcha_button.click()
        logger.info("Clicked captcha button")
        await page.wait_for_load_state("networkidle", timeout=10000)

    continue_button = await page.query_selector("#bktContinue")
    if continue_button:
        await continue_button.click()
        logger.info("Clicked Continue button after page alert")
        await page.wait_for_load_state("networkidle", timeout=20000)
    else:
        logger.error("Continue button not found after page alert")
        return None

    no_hours_selectors = [
        "text=No hay horas disponibles",
        "text=Inténtelo de nuevo dentro de unos días",
        "text=No hay horas",
        "#bktNoSlot",
        ".no-dates-message"
    ]
    
    for selector in no_hours_selectors:
        try:
            no_hours = await page.query_selector(selector)
            if no_hours:
                logger.info(f"No available dates message found immediately after Continue: {selector}")
                try:
                    await page.screenshot(path=f"no_hours_after_continue_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                except Exception:
                    pass
                return []
        except Exception:
            pass

    page_content = await page.content()
    if "No hay horas disponibles" in page_content or "Inténtelo de nuevo dentro de unos días" in page_content:
        logger.info("Found 'No hay horas disponibles' in page content after Continue")
        return []

    logger.info("Passed initial 'No hay horas' check, looking for Aceptar button")
    acceptar_button = await page.query_selector("text=Aceptar")
    if acceptar_button:
        await acceptar_button.click()
        logger.info("Clicked Aceptar button")
        await page.wait_for_load_state("networkidle", timeout=10000)
    else:
        logger.warning("Aceptar button not found - this might be normal if directly showing options")
        
    try:
        await page.wait_for_selector("#idListServices", timeout=20000)
        logger.info("Service options (children options) loaded")
    except TimeoutError:
        logger.warning("Service options not loaded in time, checking if we're at 'No hay horas' page")
        
        for selector in no_hours_selectors:
            try:
                if await page.query_selector(selector):
                    logger.info(f"No available dates message found after waiting for options: {selector}")
                    return []
            except Exception:
                pass
                
        content = await page.content()
        if "No hay horas disponibles" in content or "Inténtelo de nuevo dentro de unos días" in content:
            logger.info("Detected 'No hay horas disponibles' message after waiting for options")
            return []

    option_text = service_option
    if service_option == "Reservar Cita de Minores Ley 36":
        option_text = "INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO"

    logger.info(f"Looking for option: {option_text}")

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
                await page.wait_for_load_state("networkidle", timeout=10000)
                break
        except Exception as e:
            logger.warning(f"Error using selector {selector}: {e}")

    if not option_found:
        logger.warning(f"Option not found with standard selectors: {option_text}")
        services = await page.query_selector_all(".clsBktServiceName")
        for service in services:
            text = await service.text_content()
            if option_text.lower() in text.lower():
                await service.click()
                logger.info(f"Clicked alternative service: {text}")
                option_found = True
                await page.wait_for_load_state("networkidle", timeout=10000)
                break

    if not option_found:
        logger.warning("Could not find and click the requested service option")
        
        content = await page.content()
        if "No hay horas disponibles" in content or "Inténtelo de nuevo dentro de unos días" in content:
            logger.info("Detected 'No hay horas disponibles' message in page content")
            return []
            
        try:
            await page.screenshot(path=f"debug_screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            logger.info("Saved debug screenshot")
        except Exception as screenshot_error:
            logger.warning(f"Failed to save debug screenshot: {screenshot_error}")
            
        return None

    await page.wait_for_load_state("networkidle", timeout=30000)

    for selector in no_hours_selectors:
        try:
            no_hours = await page.query_selector(selector)
            if no_hours:
                logger.info(f"No available dates message found after selecting service: {selector}")
                return []
        except Exception:
            pass

    available_dates = await extract_dates(page, preferred_date)
    return available_dates


async def extract_dates(page, preferred_date: Optional[str] = None) -> List[str]:
    try:
        no_dates_content = [
            "No hay horas disponibles",
            "Inténtelo de nuevo dentro de unos días",
            "No hay horas",
            "No slots available",
            "Try again later"
        ]
        
        page_content = await page.content()
        for no_dates_text in no_dates_content:
            if no_dates_text in page_content:
                logger.info(f"Found 'no dates' message in content: {no_dates_text}")
                return []

        dates = await page.evaluate('''
            () => {
                const availableDates = [];
                const selectors = [
                    '.available-date', 
                    '[data-available="true"]',
                    '.calendar-day-available',
                    '.ui-state-active',
                    '.ui-state-default:not(.ui-state-disabled)',
                    '.day:not(.disabled)'
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
            calendarPresent = await page.evaluate('''
                () => {
                    return document.querySelector('.ui-datepicker') !== null || 
                           document.querySelector('.calendar') !== null ||
                           document.querySelector('.datepicker') !== null;
                }
            ''')
            
            if calendarPresent:
                dates = await page.evaluate('''
                    () => {
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

                        const availableDays = Array.from(document.querySelectorAll('.day:not(.disabled)'));
                        if (availableDays.length > 0) {
                            const currentMonth = document.querySelector('.datepicker-switch')?.innerText.trim() || '';
                            return availableDays.map(day => `${day.innerText.trim()} ${currentMonth}`);
                        }

                        return [];
                    }
                ''')
            else:
                noDateIndications = await page.evaluate('''
                    () => {
                        const pageText = document.body.innerText.toLowerCase();
                        return {
                            noHours: pageText.includes("no hay horas"),
                            tryAgain: pageText.includes("inténtelo de nuevo"),
                            noSlots: pageText.includes("no hay citas disponibles"),
                            noAppointments: pageText.includes("no appointments available")
                        };
                    }
                ''')
                
                if noDateIndications.get('noHours', False) or noDateIndications.get('tryAgain', False) or \
                   noDateIndications.get('noSlots', False) or noDateIndications.get('noAppointments', False):
                    logger.info("Text analysis indicates no dates are available")
                    return []

        if dates and len(dates) > 0:
            logger.info(f"Found {len(dates)} available dates")

            if preferred_date:
                try:
                    preferred_dt = datetime.strptime(preferred_date, "%d/%m/%Y")

                    exact_match = None
                    closest_date = None
                    min_diff = float('inf')

                    for date_str in dates:
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
                                    return [f"SELECTED: {date_str}"]

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
                    
            try:
                first_date_clickable = await page.query_selector('.ui-state-default:not(.ui-state-disabled)')
                if first_date_clickable:
                    await first_date_clickable.click()
                    logger.info("Clicked on an available date to confirm")
                    
                    await asyncio.sleep(2)
                    time_slots = await page.query_selector_all('.bktSlot, .time-slot')
                    if time_slots and len(time_slots) > 0:
                        logger.info(f"Confirmed date availability - found {len(time_slots)} time slots")
                    else:
                        logger.warning("No time slots found after clicking date - may be false positive")
            except Exception as e:
                logger.warning(f"Error trying to confirm date availability: {e}")

            return dates
            
        try:
            await page.screenshot(path=f"no_dates_screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            logger.info("Saved 'no dates' screenshot for debugging")
        except Exception as screenshot_error:
            logger.warning(f"Failed to save 'no dates' screenshot: {screenshot_error}")
        
        logger.info("No available dates found")
        return []

    except Exception as e:
        logger.error(f"Error extracting dates: {e}")
        logger.error(traceback.format_exc())
        return []