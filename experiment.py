from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import StaleElementReferenceException
import time
from multiprocessing import Pool


chromedriver_path = "chromedriver"
service = Service(chromedriver_path)
chrome_options = webdriver.ChromeOptions()
if False:
    chrome_options.add_argument("--headless")
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.get(
    "https://jobs.careers.microsoft.com/global/en/search?q=Software%20Engineer%20-principal%20-senior%20python%20-atlanta&lc=United%20States&p=Software%20Engineering&exp=Experienced%20professionals&rt=Individual%20Contributor&et=Full-Time&l=en_us&pg=1&pgSz=20&o=Relevance&flt=true"
)
time.sleep(3)

for i in range(20):
    driver.get(
        "https://jobs.careers.microsoft.com/global/en/search?q=Software%20Engineer%20-principal%20-senior%20python%20-atlanta&lc=United%20States&p=Software%20Engineering&exp=Experienced%20professionals&rt=Individual%20Contributor&et=Full-Time&l=en_us&pg=1&pgSz=20&o=Relevance&flt=true"
    )
    time.sleep(3)
    [div_element] = driver.find_elements(
        By.CSS_SELECTOR,
        f"div[role='listitem'][class='ms-List-cell'][data-list-index='{i}'][data-automationid='ListCell']",
    )
    [button] = div_element.find_elements(By.TAG_NAME, "button")
    button.click()
    current_url = driver.current_url
    print(f"URL copied to clipboard: {current_url}")

driver.quit()
