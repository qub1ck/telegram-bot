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
    """Enhanced appointment checking with robust proxy handling and error recovery."""
    proxy_manager = ProxyManager()
    max_attempts = 5  # Increased from 3 to 5
    
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
                # Configure browser launch options with adaptive retry settings
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
                
                # Handle dialogs automatically
                page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
                
                # Enhanced navigation with multiple retry strategies
                max_navigation_retries = 3
                for nav_retry in range(max_navigation_retries):
                    try:
                        await page.goto(
                            "https://www.exteriores.gob.es/Consulados/lahabana/es/ServiciosConsulares/Paginas/menorescita.aspx", 
                            timeout=30000,
                            wait_until="domcontentloaded"  # Less strict than networkidle
                        )
                        
                        # Wait for page to stabilize
                        await asyncio.sleep(2)
                        
                        # Verify we've reached the correct page
                        title = await page.title()
                        if "Consulado" not in title and "cita" not in title.lower():
                            logger.warning(f"Page title unexpected: {title}")
                            if nav_retry < max_navigation_retries - 1:
                                continue
                            else:
                                raise Exception("Failed to reach correct page after multiple attempts")
                        
                        break  # Success
                    except Exception as e:
                        logger.warning(f"Navigation retry {nav_retry+1}/{max_navigation_retries}: {str(e)}")
                        if nav_retry == max_navigation_retries - 1:
                            raise
                        await asyncio.sleep(1)  # Brief pause before retry
                
                # Click with retry mechanism
                async def click_with_retry(selector, max_retries=3, description="element"):
                    for click_retry in range(max_retries):
                        try:
                            await page.wait_for_selector(selector, state="visible", timeout=10000)
                            await page.click(selector)
                            return True
                        except Exception as e:
                            if click_retry == max_retries - 1:
                                logger.error(f"Failed to click {description} after {max_retries} attempts: {str(e)}")
                                raise
                            logger.warning(f"Click retry {click_retry+1}/{max_retries} for {description}: {str(e)}")
                            await asyncio.sleep(1)
                
                # Try to find and click the appointment button
                try:
                    # First try the text selector
                    await click_with_retry("text=Reservar cita de Menores Ley 36.", description="appointment button")
                except Exception:
                    # Fall back to looking for links with keywords
                    logger.info("Trying alternative selectors for appointment button")
                    link_found = False
                    for selector in ["a:has-text('Reservar')", "a:has-text('cita')", "a:has-text('Menores')"]:
                        try:
                            await click_with_retry(selector, description=f"alternative button ({selector})")
                            link_found = True
                            break
                        except Exception:
                            continue
                    
                    if not link_found:
                        raise Exception("Could not find any appropriate appointment links")
                
                # Wait for captcha button with extended timeout
                try:
                    await page.wait_for_selector("#idCaptchaButton", state="visible", timeout=15000)
                    await page.click("#idCaptchaButton")
                except PlaywrightTimeoutError:
                    # Try alternative approach if standard captcha button not found
                    logger.warning("Captcha button not found with standard selector, trying alternatives")
                    
                    for alt_captcha_selector in ["button:has-text('Captcha')", "button:has-text('Continuar')", "input[type=button]"]:
                        try:
                            await page.wait_for_selector(alt_captcha_selector, state="visible", timeout=5000)
                            await page.click(alt_captcha_selector)
                            logger.info(f"Used alternative captcha selector: {alt_captcha_selector}")
                            break
                        except Exception:
                            continue
                
                # Wait for page to load with more resilience
                try:
                    await page.wait_for_load_state("networkidle", timeout=20000)
                except PlaywrightTimeoutError:
                    logger.warning("networkidle timeout, continuing anyway")
                
                # Try multiple approaches to find and click the continue button
                try:
                    await click_with_retry("#bktContinue", description="continue button")
                except Exception:
                    logger.warning("Standard continue button not found, trying alternatives")
                    for continue_selector in ["button:has-text('Continue')", "button:has-text('Continuar')", "input[type=submit]"]:
                        try:
                            await click_with_retry(continue_selector, description=f"alternative continue ({continue_selector})")
                            break
                        except Exception:
                            continue
                
                # Wait for services list with resilience
                try:
                    await page.wait_for_selector("#idListServices", state="visible", timeout=15000)
                except PlaywrightTimeoutError:
                    # Try alternatives if standard selector fails
                    logger.warning("Service list not found with standard selector")
                    service_found = False
                    for service_selector in [".service-list", ".services", "div.services-container"]:
                        try:
                            await page.wait_for_selector(service_selector, state="visible", timeout=5000)
                            service_found = True
                            break
                        except Exception:
                            continue
                    
                    if not service_found:
                        screenshot = await page.screenshot(type="jpeg", quality=50)
                        logger.error(f"Page content: {await page.content()}")
                        raise Exception("Could not find services list with any selector")
                
                # Try to find the service option with multiple approaches
                service_found = False
                
                # First try: exact XPath with the provided user_choice
                option_xpath = f"//div[@class='clsBktServiceName clsHP']/a[contains(text(), '{user_choice}')]"
                try:
                    await page.wait_for_selector(option_xpath, timeout=5000)
                    await page.click(option_xpath)
                    service_found = True
                except Exception:
                    logger.warning(f"Service not found with primary XPath: {option_xpath}")
                
                # Second try: Look for keywords in the service name if exact match fails
                if not service_found:
                    keywords = ["MENOR", "LEY36", "HIJO", "HIJOS"]
                    for keyword in keywords:
                        try:
                            keyword_xpath = f"//div[contains(@class, 'Service')]/a[contains(text(), '{keyword}')]"
                            await page.wait_for_selector(keyword_xpath, timeout=3000)
                            await page.click(keyword_xpath)
                            service_found = True
                            logger.info(f"Found service using keyword: {keyword}")
                            break
                        except Exception:
                            continue
                
                if not service_found:
                    # As a last resort, log available services for debugging
                    services = await page.evaluate("""
                        () => {
                            const services = [];
                            document.querySelectorAll('div.clsBktServiceName a').forEach(el => {
                                services.push(el.textContent.trim());
                            });
                            return services;
                        }
                    """)
                    logger.error(f"Available services: {services}")
                    raise Exception(f"Could not find service matching '{user_choice}' or any keywords")
                
                # Enhanced availability check with multiple approaches
                no_hours_available = False
                
                # First check: Look for the explicit "No hay horas disponibles" message
                try:
                    no_hours_message = await page.query_selector("text=No hay horas disponibles")
                    if no_hours_message:
                        no_hours_available = True
                except Exception:
                    pass
                
                # Second check: Check if there are any available date elements
                if not no_hours_available:
                    available_dates = await page.evaluate('''() => {
                        const dates = [];
                        document.querySelectorAll('.available-date, [data-available="true"], .calendar-day-available').forEach(dateElement => {
                            dates.push(dateElement.innerText.trim());
                        });
                        return dates.filter(d => d && d.length > 0);
                    }''')
                    
                    if available_dates and len(available_dates) > 0:
                        logger.info(f"Found {len(available_dates)} available dates: {available_dates}")
                        return available_dates
                    else:
                        no_hours_available = True
                
                logger.info("No available dates found.")
                return None
                
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

# For testing
if __name__ == "__main__":
    import sys
    
    async def test():
        choice = "INSCRIPCIÓN MENORES LEY36 OPCIÓN 1 HIJO"
        if len(sys.argv) > 1:
            choice = sys.argv[1]
        
        logger.info(f"Testing with choice: {choice}")
        result = await check_appointments_async(choice)
        logger.info(f"Result: {result}")
    
    asyncio.run(test())
