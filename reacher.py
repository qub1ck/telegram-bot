import asyncio
import random
import logging
from typing import List, Optional, Dict, Tuple
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProxyManager:
    """Manage proxy loading, selection, and rotation."""
    def __init__(self, proxy_file: str = "proxy.txt"):
        self.proxy_file = proxy_file
        self.proxies: List[Tuple[str, str]] = []
        self.used_proxies: List[Tuple[str, str]] = []
        
    async def load_proxies(self) -> List[Tuple[str, str]]:
        """Load proxies from file with error handling."""
        try:
            async with aiofiles.open(self.proxy_file, mode='r') as f:
                proxies = [
                    tuple(line.strip().split(':')) 
                    for line in await f.readlines() 
                    if ':' in line
                ]
            logger.info(f"Loaded {len(proxies)} proxies")
            return proxies
        except FileNotFoundError:
            logger.error(f"Proxies file {self.proxy_file} not found")
            return []
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
            return []
    
    async def get_proxy(self) -> Optional[Dict[str, str]]:
        """Select a proxy with rotation strategy."""
        if not self.proxies:
            self.proxies = await self.load_proxies()
        
        if not self.proxies:
            logger.warning("No proxies available")
            return None
        
        proxy = random.choice(self.proxies)
        self.proxies.remove(proxy)
        self.used_proxies.append(proxy)
        
        return {
            "server": f"{proxy[0]}:{proxy[1]}",
            "username": "vqytkifr",
            "password": "x90e6lupyath"
        }
    
    def reset_proxies(self):
        """Reset proxy pool after exhaustion."""
        self.proxies.extend(self.used_proxies)
        self.used_proxies.clear()

async def check_appointments_async(user_choice: str) -> Optional[List[str]]:
    """Enhanced appointment checking with robust proxy handling."""
    proxy_manager = ProxyManager()
    max_attempts = 3
    
    for attempt in range(max_attempts):
        try:
            proxy_options = await proxy_manager.get_proxy()
            
            async with async_playwright() as p:
                browser_args = {
                    "headless": True,
                    "proxy": proxy_options if proxy_options else None
                }
                
                browser = await p.chromium.launch(**browser_args)
                context = await browser.new_context(
                    viewport={"width": 1280, "height": 720},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
                page = await context.new_page()
                
                logger.info(f"Attempt {attempt + 1}: Checking appointments for {user_choice}")
                
                try:
                    await page.goto(
                        "https://www.exteriores.gob.es/Consulados/lahabana/es/ServiciosConsulares/Paginas/menorescita.aspx", 
                        timeout=30000,
                        wait_until="networkidle"
                    )
                    
                    await page.click("text=Reservar cita de Menores Ley 36.")
                    await page.wait_for_selector("#idCaptchaButton", state="visible", timeout=10000)
                    
                    page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
                    await page.click("#idCaptchaButton")
                    
                    await page.wait_for_load_state("networkidle")
                    await page.evaluate('''() => {
                        const widget = document.querySelector('#idBktDefaultServicesContainer')
                    }''')
                    
                    await page.click("#bktContinue")
                    await page.wait_for_selector("#idListServices", state="visible", timeout=10000)
                    
                    option_xpath = f"//div[@class='clsBktServiceName clsHP']/a[contains(text(), '{user_choice}')]"
                    await page.click(option_xpath)
                    
                    # Enhanced availability check
                    no_hours_message = await page.query_selector("text=No hay horas disponibles")
                    if no_hours_message:
                        logger.info("No available dates found.")
                        return None
                    
                    available_dates = await page.evaluate('''() => {
                        const dates = [];
                        document.querySelectorAll('.available-date').forEach(dateElement => {
                            dates.push(dateElement.innerText);
                        });
                        return dates;
                    }''')
                    
                    if available_dates:
                        logger.info(f"Found {len(available_dates)} available dates")
                        return available_dates
                    
                    return None
                
                except Exception as page_error:
                    logger.error(f"Page processing error (Attempt {attempt + 1}): {page_error}")
                    continue
                
                finally:
                    await browser.close()
        
        except Exception as e:
            logger.error(f"Overall check error (Attempt {attempt + 1}): {e}")
            continue
    
    logger.error("Failed to check appointments after maximum attempts")
    return None

# Optional: Adding aiofiles for async file operations
import aiofiles
