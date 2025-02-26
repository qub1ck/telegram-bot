import asyncio
import random
from playwright.async_api import async_playwright
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reacher")

async def load_proxies(filename):
    try:
        with open(filename, 'r') as f:
            return [line.strip().split(':') for line in f if ':' in line]
    except FileNotFoundError:
        logger.error(f"Proxies file {filename} not found")
        return []

async def check_appointments_async(user_choice):
    proxies = await load_proxies("proxy.txt")
    if proxies:
        ip, port = random.choice(proxies)
        proxy_str = f"{ip}:{port}"
        proxy_options = {
            "server": proxy_str,
            "username": "vqytkifr",  # Proxy username
            "password": "x90e6lupyath"  # Proxy password
        }
        logger.info(f"Using proxy: {proxy_str}")
    else:
        proxy_options = None
        logger.info("No proxies available, running without proxy")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, proxy=proxy_options)
        page = await browser.new_page()

        try:
            logger.info("Starting appointment check for: %s", user_choice)
            await page.goto("https://www.exteriores.gob.es/Consulados/lahabana/es/ServiciosConsulares/Paginas/menorescita.aspx")
            await page.wait_for_load_state("networkidle")
            await page.click("text=Reservar cita de Menores Ley 36.")
            page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
            await page.wait_for_selector("#idCaptchaButton", state="visible", timeout=300)
            await page.click("#idCaptchaButton")
            await page.wait_for_load_state("networkidle")
            await page.evaluate('''() => {
                const widget = document.querySelector('#idBktDefaultServicesContainer');
                if (widget) {
                    widget.scrollTop = widget.scrollHeight;
                }
            }''')
            await page.click("#bktContinue")
            await page.wait_for_selector("#idListServices", state="visible", timeout=300)
            option_xpath = f"//div[@class='clsBktServiceName clsHP']/a[contains(text(), '{user_choice}')]"
            await page.click(option_xpath)
            no_hours_message = await page.query_selector("text=No hay horas disponibles")
            if no_hours_message:
                logger.info("No available dates found.")
                return None
            else:
                available_dates = await page.evaluate('''() => {
                    const dates = [];
                    document.querySelectorAll('.available-date').forEach(dateElement => {
                        dates.push(dateElement.innerText);
                    });
                    return dates;
                }''')
                return available_dates
        except Exception as e:
            logger.error("Error in check_appointments_async: %s", str(e))
            return None
        finally:
            await browser.close()