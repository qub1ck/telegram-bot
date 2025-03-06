import asyncio
import random
import logging
import traceback
from typing import List, Optional, Dict, Tuple
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProxyManager:
    """Manage proxy loading, selection, and rotation."""
    def __init__(self, proxy_file: str = "proxy.txt"):
        self.proxy_file = proxy_file
        self.proxies: List[Tuple[str, str]] = []
        self.used_proxies: List[Tuple[str, str]] = []
        self.failed_proxies: Dict[Tuple[str, str], int] = {}
        
    async def load_proxies(self) -> List[Tuple[str, str]]:
        """Load proxies from file with error handling."""
        try:
            import aiofiles
            async with aiofiles.open(self.proxy_file, mode='r') as f:
                content = await f.read()
                proxies = []
                for line in content.strip().split('\n'):
                    if ':' in line:
                        host_port = tuple(line.strip().split(':'))
                        proxies.append(host_port)
            
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
            
            # If still no proxies, try without a proxy
            if not self.proxies:
                logger.warning("No proxies available, proceeding without proxy")
                return None
        
        # Try to find a proxy that hasn't failed too many times
        good_proxies = [p for p in self.proxies if self.failed_proxies.get(p, 0) < 3]
        
        if not good_proxies:
            # If all proxies have failed too many times, reset failure counts
            logger.warning("All proxies have excessive failures, resetting failure counts")
            self.failed_proxies.clear()
            good_proxies = self.proxies
            
        proxy = random.choice(good_proxies)
        self.proxies.remove(proxy)
        self.used_proxies.append(proxy)
        
        return {
            "server": f"{proxy[0]}:{proxy[1]}",
            "username": "vqytkifr",
            "password": "x90e6lupyath"
        }
    
    def mark_proxy_failed(self, proxy: Dict[str, str]):
        """Mark a proxy as failed to reduce its chances of being selected again."""
        if not proxy:
            return
            
        server = proxy.get("server", "")
        if not server or ":" not in server:
            return
            
        host, port = server.split(":")
        proxy_tuple = (host, port)
        
        self.failed_proxies[proxy_tuple] = self.failed_proxies.get(proxy_tuple, 0) + 1
        logger.info(f"Marked proxy {server} as failed (count: {self.failed_proxies[proxy_tuple]})")
    
    def reset_proxies(self):
        """Reset proxy pool after exhaustion."""
        self.proxies.extend(self.used_proxies)
        self.used_proxies.clear()

async def check_appointments_async(user_choice: str) -> Optional[List[str]]:
    """Enhanced appointment checking with proper page flow handling based on actual HTML structure."""
    proxy_manager = ProxyManager()
    max_attempts = 5
    
    for attempt in range(max_attempts):
        proxy_options = None
        browser = None
        context = None
        
        try:
            # Only use proxy on 2nd attempt and beyond to try a direct connection first
            if attempt > 0:
                proxy_options = await proxy_manager.get_proxy()
                logger.info(f"Attempt {attempt+1}/{max_attempts}: Using proxy: {proxy_options['server'] if proxy_options else 'None'}")
            else:
                logger.info(f"Attempt {attempt+1}/{max_attempts}: Trying direct connection (no proxy)")
            
            async with async_playwright() as p:
                browser_args = {
                    "headless": True,
                }
                
                if proxy_options:
                    browser_args["proxy"] = proxy_options
                
                # Use different browser types on different attempts for resilience
                browser_type = p.chromium
                if attempt % 3 == 1:
                    browser_type = p.firefox
                elif attempt % 3 == 2:
                    browser_type = p.webkit
                
                browser = await browser_type.launch(**browser_args)
                
                # Configure browser context with randomized fingerprinting
                context = await browser.new_context(
                    viewport={"width": 1280, "height": 720},
                    user_agent=random.choice([
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0"
                    ]),
                    locale=random.choice(["en-US", "en-GB", "es-ES"]),
                    timezone_id=random.choice(["Europe/Madrid", "Europe/London", "America/New_York"]),
                )
                
                # Set longer timeouts for more resilience
                context.set_default_timeout(45000)  # 45 seconds
                
                page = await context.new_page()
                
                # Intercept and log all console messages for debugging
                page.on("console", lambda msg: logger.debug(f"Browser console {msg.type}: {msg.text}"))
                
                # Navigate to the appointment page with retry mechanism
                max_navigation_retries = 3
                for nav_retry in range(max_navigation_retries):
                    try:
                        await page.goto(
                            "https://www.exteriores.gob.es/Consulados/lahabana/es/ServiciosConsulares/Paginas/menorescita.aspx", 
                            timeout=30000,
                            wait_until="domcontentloaded"
                        )
                        
                        # Verify we've reached the correct page
                        title = await page.title()
                        if "Consulado" not in title and "cita" not in title.lower():
                            logger.warning(f"Page title unexpected: {title}")
                            if nav_retry < max_navigation_retries - 1:
                                await asyncio.sleep(1)
                                continue
                            else:
                                raise Exception("Failed to reach correct page after multiple attempts")
                        
                        break  # Success
                    except Exception as e:
                        logger.warning(f"Navigation retry {nav_retry+1}/{max_navigation_retries}: {str(e)}")
                        if nav_retry == max_navigation_retries - 1:
                            raise
                        await asyncio.sleep(2)
                
                # Wait for page to stabilize
                await asyncio.sleep(2)
                
                # Find and click the appointment link
                logger.info("Looking for appointment link...")
                appointment_link_clicked = False
                
                # Try different selectors for the appointment link
                appointment_selectors = [
                    "text=Reservar cita de Menores Ley 36.",
                    "a:has-text('Reservar cita de Menores')",
                    "a:has-text('Ley 36')",
                    "a:has-text('Menores')"
                ]
                
                for selector in appointment_selectors:
                    try:
                        await page.wait_for_selector(selector, state="visible", timeout=5000)
                        await page.click(selector)
                        logger.info(f"Clicked appointment link with selector: {selector}")
                        appointment_link_clicked = True
                        break
                    except Exception as e:
                        logger.warning(f"Failed to click with selector {selector}: {str(e)}")
                
                if not appointment_link_clicked:
                    # Take a screenshot and log what's on the page
                    await page.screenshot(path=f"debug_initial_page_{attempt}.png")
                    page_content = await page.content()
                    logger.error(f"Initial page content: {page_content[:1000]}...")
                    raise Exception("Could not find or click appointment link")
                
                # Set up dialog handler now that we're going to interact with the dialog
                page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
                
                # Wait for the captcha page
                logger.info("Waiting for captcha container...")
                await page.wait_for_selector("#idCaptchaContainer", state="visible", timeout=15000)
                
                # Click the captcha button
                logger.info("Clicking captcha button...")
                await page.click("#idCaptchaButton")
                
                # Wait for terms dialog to appear
                logger.info("Waiting for terms dialog to appear...")
                try:
                    await page.wait_for_selector("#dialog-confirm", state="visible", timeout=15000)
                    logger.info("Terms dialog found")
                except Exception as e:
                    logger.warning(f"Terms dialog not found: {e}")
                    # Take a screenshot to see what's on the page
                    await page.screenshot(path=f"debug_after_captcha_{attempt}.png")
                    raise Exception("Terms dialog not found after captcha")
                
                # Wait a moment to ensure dialog is fully loaded
                await asyncio.sleep(1)
                
                # Now click the ACEPTAR button in the dialog (which is a div with ID bktContinue)
                logger.info("Clicking ACEPTAR button...")
                try:
                    # Try direct selector first
                    await page.click("#bktContinue")
                    logger.info("Clicked #bktContinue successfully")
                except Exception as e:
                    logger.warning(f"Failed to click #bktContinue directly: {e}")
                    
                    # Try JavaScript click as fallback
                    try:
                        await page.evaluate('''() => {
                            const continueBtn = document.querySelector("#bktContinue");
                            if (continueBtn) {
                                continueBtn.click();
                                return true;
                            }
                            return false;
                        }''')
                        logger.info("Clicked #bktContinue via JavaScript")
                    except Exception as js_e:
                        logger.error(f"JavaScript click failed: {js_e}")
                        raise Exception("Failed to click ACEPTAR button after multiple attempts")
                
                # Wait for services list to appear
                logger.info("Waiting for services list to appear...")
                try:
                    await page.wait_for_selector("#idListServices", state="visible", timeout=15000)
                    logger.info("Services list found")
                except Exception as e:
                    logger.warning(f"Services list not found: {e}")
                    
                    # Try to make services list visible with JavaScript if it exists but is hidden
                    try:
                        is_hidden = await page.evaluate('''() => {
                            const servicesList = document.querySelector("#idListServices");
                            if (servicesList && servicesList.style.display === "none") {
                                servicesList.style.display = "block";
                                return true;
                            }
                            return false;
                        }''')
                        
                        if is_hidden:
                            logger.info("Made services list visible via JavaScript")
                            # Wait a moment for the display change to take effect
                            await asyncio.sleep(1)
                        else:
                            # Take a screenshot to see what's on the page
                            await page.screenshot(path=f"debug_no_services_{attempt}.png")
                            raise Exception("Services list not found and couldn't be made visible")
                    except Exception as js_e:
                        logger.error(f"JavaScript visibility toggle failed: {js_e}")
                        raise Exception("Failed to find or show services list")
                
                # Now find and click the specific service option
                logger.info(f"Looking for service option: {user_choice}")
                service_option_found = False
                
                # Based on the HTML, we can see the exact structure for the service links
                service_selector = f"//div[@class='clsBktServiceName clsHP']/a[contains(text(), '{user_choice}')]"
                
                try:
                    await page.wait_for_selector(service_selector, state="visible", timeout=10000)
                    await page.click(service_selector)
                    logger.info(f"Clicked service option: {user_choice}")
                    service_option_found = True
                except Exception as e:
                    logger.warning(f"Failed to find service with primary selector: {e}")
                    
                    # Try alternative selectors if the first one fails
                    alternative_selectors = [
                        f"a:has-text('{user_choice}')",
                        f"a:has-text('OPCIÓN {user_choice.split('OPCIÓN')[1].strip() if 'OPCIÓN' in user_choice else ''}')"
                    ]
                    
                    for alt_selector in alternative_selectors:
                        try:
                            await page.wait_for_selector(alt_selector, state="visible", timeout=5000)
                            await page.click(alt_selector)
                            logger.info(f"Clicked service option with alternative selector: {alt_selector}")
                            service_option_found = True
                            break
                        except Exception:
                            continue
                
                if not service_option_found:
                    # Log available services for debugging
                    try:
                        services = await page.evaluate("""
                            () => {
                                const services = [];
                                document.querySelectorAll('.clsBktServiceName a').forEach(el => {
                                    services.push(el.innerText.trim());
                                });
                                return services;
                            }
                        """)
                        logger.error(f"Available services: {services}")
                    except Exception as e:
                        logger.error(f"Failed to get services list: {e}")
                    
                    raise Exception(f"Could not find service option for '{user_choice}'")
                
                # Wait for calendar/availability page
                logger.info("Waiting for availability information...")
                await page.wait_for_load_state("networkidle", timeout=15000)
                
                # Check for availability
                logger.info("Checking for available dates")
                try:
                    no_hours_message = await page.query_selector("text=No hay horas disponibles")
                    no_hours_div = await page.query_selector("div:has-text('No hay horas disponibles')")
                    
                    if no_hours_message or no_hours_div:
                        logger.info("No available dates found (explicit message).")
                        return None
                    
                    # Check for available date elements
                    available_dates = await page.evaluate('''() => {
                        const dates = [];
                        document.querySelectorAll('.available-date, [data-available="true"], .calendar-day-available').forEach(dateElement => {
                            dates.push(dateElement.innerText.trim());
                        });
                        return dates.filter(d => d && d.length > 0);
                    }''')

                    # After finding available dates (around line 350):
                    if available_dates and len(available_dates) > 0:
                        logger.info(f"Found {len(available_dates)} available dates: {available_dates}")

                        # If there's a preferred date, try to select it or find the closest
                        if preferred_date:
                            try:
                                # Parse the preferred date string into a datetime object
                                preferred_datetime = datetime.strptime(preferred_date, "%d/%m/%Y")

                                # Find exact match or closest date
                                exact_match = None
                                closest_date = None
                                min_difference = float('inf')

                                for date_str in available_dates:
                                    # Extract date part from combined string and parse
                                    date_part = date_str.split(" - ")[0].strip()
                                    try:
                                        # Spanish date format parsing
                                        for format_str in ["%A %d de %B de %Y", "%d/%m/%Y"]:
                                            try:
                                                current_date = datetime.strptime(date_part, format_str)
                                                break
                                            except ValueError:
                                                continue
                                            
                                        # Check for exact match
                                        if current_date.date() == preferred_datetime.date():
                                            exact_match = date_str
                                            # Try to select this slot by clicking its link
                                            # Extract time from date string
                                            time_part = date_str.split(" - ")[1].split(" ")[0].strip()
                                            # Click the slot
                                            slot_selector = f"a[href*='{current_date.strftime('%Y-%m-%d')}/{time_part}']"
                                            try:
                                                await page.click(slot_selector)
                                                logger.info(f"Selected exact preferred date: {date_str}")
                                                return [f"SELECTED: {date_str}"]
                                            except Exception as e:
                                                logger.error(f"Error selecting preferred date: {e}")
                                                # Continue and return all available dates
                                                break
                                        else:
                                            # Calculate how close this date is to the preferred
                                            difference = abs((current_date.date() - preferred_datetime.date()).days)
                                            if difference < min_difference:
                                                min_difference = difference
                                                closest_date = date_str
                                    except Exception as parse_err:
                                        logger.warning(f"Error parsing date {date_part}: {parse_err}")
                                        continue
                                    
                                # If no exact match but found closest, try to select it
                                if not exact_match and closest_date:
                                    try:
                                        date_part = closest_date.split(" - ")[0].strip()
                                        time_part = closest_date.split(" - ")[1].split(" ")[0].strip()

                                        # Parse the closest date
                                        for format_str in ["%A %d de %B de %Y", "%d/%m/%Y"]:
                                            try:
                                                closest_datetime = datetime.strptime(date_part, format_str)
                                                break
                                            except ValueError:
                                                continue

                                        # Click the slot
                                        slot_selector = f"a[href*='{closest_datetime.strftime('%Y-%m-%d')}/{time_part}']"
                                        await page.click(slot_selector)
                                        logger.info(f"Selected closest available date: {closest_date}")
                                        return [f"SELECTED (closest available): {closest_date}"]
                                    except Exception as e:
                                        logger.error(f"Error selecting closest date: {e}")
                                        # Continue and return all available dates

                            except Exception as e:
                                logger.error(f"Error processing preferred date: {e}")
                                # Continue and return all available dates without selection

                        # Return all available dates if no selection was made
                        return available_dates
                    else:
                        if available_dates and len(available_dates) > 0:
                            logger.info(f"Found {len(available_dates)} available dates: {available_dates}")
                            return available_dates
                        else:
                            logger.info("No available dates found (no available date elements).")
                            return None
                    
                except Exception as e:
                    logger.error(f"Error checking availability: {e}")
                    await page.screenshot(path=f"debug_availability_error_{attempt}.png")
                    return None  # Return None on error to be safe
                
        except Exception as e:
            if proxy_options:
                proxy_manager.mark_proxy_failed(proxy_options)
            
            logger.error(f"Attempt {attempt+1}/{max_attempts} failed: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Sleep with exponential backoff
            if attempt < max_attempts - 1:
                backoff_time = 2 ** attempt
                logger.info(f"Retrying in {backoff_time} seconds...")
                await asyncio.sleep(backoff_time)
        
        finally:
            # Ensure resources are cleaned up
            try:
                if context:
                    await context.close()
                if browser:
                    await browser.close()
            except Exception as cleanup_error:
                logger.warning(f"Error during cleanup: {cleanup_error}")
    
    logger.error("Failed to check appointments after maximum attempts")
    return None
