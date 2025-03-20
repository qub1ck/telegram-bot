import asyncio
import random
import logging
from datetime import datetime
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
            if not self.proxies:
                logger.warning("No proxies available, proceeding without proxy")
                return None

        good_proxies = [p for p in self.proxies if self.failed_proxies.get(p, 0) < 3]
        if not good_proxies:
            logger.warning("All proxies have excessive failures, no more proxies available")
            return None

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


async def check_appointments_async(user_choice: str, preferred_date: Optional[str] = None) -> Optional[List[str]]:
    """Check appointments with proxy switching on failure until success or proxies are exhausted."""
    proxy_manager = ProxyManager()

    while True:
        proxy_options = await proxy_manager.get_proxy() if proxy_manager.used_proxies else None  # First attempt is direct
        browser = None
        context = None

        try:
            logger.info(
                f"Attempting with {'proxy ' + proxy_options['server'] if proxy_options else 'direct connection'}")

            async with async_playwright() as p:
                browser_args = {"headless": True}
                if proxy_options:
                    browser_args["proxy"] = proxy_options

                browser = await p.chromium.launch(**browser_args)
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
                context.set_default_timeout(45000)

                page = await context.new_page()
                page.on("console", lambda msg: logger.debug(f"Browser console {msg.type}: {msg.text}"))

                # Navigate with retries
                max_navigation_retries = 3
                for nav_retry in range(max_navigation_retries):
                    try:
                        await page.goto(
                            "https://www.exteriores.gob.es/Consulados/lahabana/es/ServiciosConsulares/Paginas/menorescita.aspx",
                            timeout=60000,
                            wait_until="domcontentloaded"
                        )
                        title = await page.title()
                        if "Consulado" not in title and "cita" not in title.lower():
                            logger.warning(f"Page title unexpected: {title}")
                            if nav_retry < max_navigation_retries - 1:
                                await asyncio.sleep(1)
                                continue
                            raise Exception("Failed to reach correct page")
                        break
                    except Exception as e:
                        logger.warning(f"Navigation retry {nav_retry + 1}/{max_navigation_retries}: {str(e)}")
                        if nav_retry == max_navigation_retries - 1:
                            raise

                await asyncio.sleep(2)

                # Click appointment link
                logger.info("Looking for appointment link...")
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
                        break
                    except Exception:
                        continue
                else:
                    raise Exception("Could not find or click appointment link")

                page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
                logger.info("Waiting for captcha container...")
                await page.wait_for_selector("#idCaptchaContainer", state="visible", timeout=15000)
                await page.click("#idCaptchaButton")

                logger.info("Waiting for terms dialog...")
                await page.wait_for_selector("#dialog-confirm", state="visible", timeout=15000)
                await page.click("#bktContinue")

                logger.info("Waiting for services list...")
                await page.wait_for_selector("#idListServices", state="visible", timeout=15000)

                # Select service
                logger.info(f"Looking for service option: {user_choice}")
                service_selector = f"//div[@class='clsBktServiceName clsHP']/a[contains(text(), '{user_choice}')]"
                try:
                    await page.wait_for_selector(service_selector, state="visible", timeout=10000)
                    await page.click(service_selector)
                except Exception:
                    alternative_selectors = [
                        f"a:has-text('{user_choice}')",
                        f"a:has-text('OPCIÓN {user_choice.split('OPCIÓN')[1].strip() if 'OPCIÓN' in user_choice else ''}')"
                    ]
                    for alt_selector in alternative_selectors:
                        try:
                            await page.wait_for_selector(alt_selector, state="visible", timeout=5000)
                            await page.click(alt_selector)
                            logger.info(f"Clicked service option with alternative selector: {alt_selector}")
                            break
                        except Exception:
                            continue
                    else:
                        services = await page.evaluate("""() => {
                            const services = [];
                            document.querySelectorAll('.clsBktServiceName a').forEach(el => services.push(el.innerText.trim()));
                            return services;
                        }""")
                        logger.error(f"Available services: {services}")
                        raise Exception(f"Could not find service option for '{user_choice}'")

                # Check availability
                logger.info("Waiting for availability information...")
                await page.wait_for_load_state("networkidle", timeout=15000)

                no_hours_message = await page.query_selector("text=No hay horas disponibles")
                if no_hours_message:
                    logger.info("No available dates found.")
                    return None

                available_dates = await page.evaluate('''() => {
                    const dates = [];
                    document.querySelectorAll('.available-date, [data-available="true"], .calendar-day-available').forEach(dateElement => {
                        dates.push(dateElement.innerText.trim());
                    });
                    return dates.filter(d => d && d.length > 0);
                }''')

                if available_dates and len(available_dates) > 0:
                    logger.info(f"Found {len(available_dates)} available dates: {available_dates}")
                    if preferred_date:
                        try:
                            preferred_datetime = datetime.strptime(preferred_date, "%d/%m/%Y")
                            exact_match = None
                            closest_date = None
                            min_difference = float('inf')

                            for date_str in available_dates:
                                date_part = date_str.split(" - ")[0].strip() if " - " in date_str else date_str
                                for format_str in ["%A %d de %B de %Y", "%d/%m/%Y"]:
                                    try:
                                        current_date = datetime.strptime(date_part, format_str)
                                        break
                                    except ValueError:
                                        continue

                                if current_date.date() == preferred_datetime.date():
                                    exact_match = date_str
                                    if " - " in date_str:
                                        time_part = date_str.split(" - ")[1].split(" ")[0].strip()
                                        slot_selector = f"a[href*='{current_date.strftime('%Y-%m-%d')}/{time_part}']"
                                        await page.click(slot_selector)
                                        logger.info(f"Selected exact preferred date: {date_str}")
                                        return [f"SELECTED: {date_str}"]
                                else:
                                    difference = abs((current_date.date() - preferred_datetime.date()).days)
                                    if difference < min_difference:
                                        min_difference = difference
                                        closest_date = date_str

                            if not exact_match and closest_date:
                                date_part = closest_date.split(" - ")[
                                    0].strip() if " - " in closest_date else closest_date
                                for format_str in ["%A %d de %B de %Y", "%d/%m/%Y"]:
                                    try:
                                        closest_datetime = datetime.strptime(date_part, format_str)
                                        break
                                    except ValueError:
                                        continue
                                if " - " in closest_date:
                                    time_part = closest_date.split(" - ")[1].split(" ")[0].strip()
                                    slot_selector = f"a[href*='{closest_datetime.strftime('%Y-%m-%d')}/{time_part}']"
                                    await page.click(slot_selector)
                                    logger.info(f"Selected closest available date: {closest_date}")
                                    return [f"SELECTED (closest available): {closest_date}"]
                                return [f"CLOSEST AVAILABLE: {closest_date}"] + available_dates

                        except Exception as e:
                            logger.error(f"Error processing preferred date: {e}")
                    return available_dates
                logger.info("No available dates found.")
                return None

        except Exception as e:
            if proxy_options:
                proxy_manager.mark_proxy_failed(proxy_options)
            logger.error(
                f"Attempt failed with {'proxy ' + proxy_options['server'] if proxy_options else 'direct connection'}: {e}")
            if proxy_options is None and not proxy_manager.proxies and not proxy_manager.used_proxies:
                logger.error("Failed to check appointments with direct connection and no proxies available")
                return None
            await asyncio.sleep(2)  # Backoff before next attempt
        finally:
            try:
                if context:
                    await context.close()
                if browser:
                    await browser.close()
            except Exception as cleanup_error:
                logger.warning(f"Error during cleanup: {cleanup_error}")