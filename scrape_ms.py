from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.chrome.webdriver import WebDriver
from pathlib import Path
import time
import re
import math
import typing as tp
import logging
from functools import partial
from multiprocessing import Pool
from tqdm.contrib.concurrent import process_map  # or thread_map
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)


class ScrapeMS:
    _WAIT_DEFAULT = 5
    _TOTAL_RESULTS_PREFIX = "Showing 1-20 of"
    _PAGE_SUBQUERY_TEMPLATE = "&pg={page_num}&"
    _JOB_BOX_TEMPLATE = "div[role='listitem'][class='ms-List-cell'][data-list-index='{job_num}'][data-automationid='ListCell']"
    _LISTING_FIELDS = (
        "Date posted",
        "Job number",
        "Work site",
        "Travel",
        "Role type",
        "Profession",
        "Discipline",
        "Employment type",
    )

    def __init__(
        self,
        base_url: str,
        chromedriver_path: Path,
        page_load_wait: int = _WAIT_DEFAULT,
        headless: bool = True,
        max_workers: int | None = None,
    ):
        # Webdrivers are not thread-safe, so store a pre-configured factory for use in multiprocessing.
        self._driver_factory: tp.Callable = self._build_driver_factory(
            chromedriver_path=chromedriver_path, headless=headless
        )

        self._base_url = base_url
        self._max_workers = max_workers

    @classmethod
    def _build_driver(
        cls, chromedriver_path: Path, headless: bool
    ) -> WebDriver:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--window-size=1600,1000")
        if headless:
            chrome_options.add_argument("--headless")

        service = service = Service(chromedriver_path)

        return webdriver.Chrome(service=service, options=chrome_options)

    @classmethod
    def _build_driver_factory(cls, chromedriver_path: Path, headless: bool) -> tp.Callable:
        return partial(cls._build_driver, chromedriver_path=chromedriver_path, headless=headless)

    @classmethod
    def _get_url(cls, url: str, driver: WebDriver) -> None:
        """
        Loads a `url` onto the provided `driver` enforcing a waiting period.
        """
        driver.get(url)
        time.sleep(cls._WAIT_DEFAULT)

    def _build_page_to_results_map(self) -> dict[int, int]:
        """
        Determine number of pages and job listings per page.
        """
        driver = self._driver_factory()
        self._get_url(self._base_url, driver=driver)
        h1_element = driver.find_element(
            By.XPATH, f"//h1[contains(., '{self._TOTAL_RESULTS_PREFIX}')]"
        )
        _, page_size, total_results = map(int, re.findall(r"\d+", h1_element.text))

        total_pages = math.ceil(total_results / page_size)
        last_page_total = total_results % page_size

        page_to_results_map = {page: page_size for page in range(1, total_pages)}
        page_to_results_map[total_pages] = last_page_total

        logging.info(
            f"Located {total_results} total listing(s) over {total_pages} pages."
        )
        return page_to_results_map

    def _get_listing_html(
        self,
        job_num: int,
        page_url: str,
    ) -> tuple[str, dict[str, str]]:
        driver = self._driver_factory()
        self._get_url(url=page_url, driver=driver)
        [div_element] = driver.find_elements(
            By.CSS_SELECTOR, self._JOB_BOX_TEMPLATE.format(job_num=job_num)
        )
        [button] = div_element.find_elements(By.TAG_NAME, "button")
        button.click()
        time.sleep(2)

        lfs = {}
        for lf in self._LISTING_FIELDS:
            print(lf)
            breakpoint()
            try:
                heading = driver.find_element(By.XPATH, f"//div[text()='{lf}']")
            except:
                breakpoint()
            value = heading.find_element(By.XPATH, "following-sibling::*[1]")
            lfs[lf] = value.text
        breakpoint()

        first_horizontal_line, *_ = driver.find_elements(By.CSS_SELECTOR, "hr[class^='horizontalLine-']")
        next_div, *_ = first_horizontal_line.find_elements(By.XPATH, "following-sibling::div")
        overview, qualifications, responsibilities, *_ = next_div.find_elements(By.XPATH, "./div")

        breakpoint()

    def _get_listing_text_highlight(
        self,
        job_num: int,
        page_url: str,
    ) -> tuple[str, str]:
        """
        Get the text of the given `job_num` on the given `page_url`.
        """
        time.sleep(job_num)
        url = ""
        logging.debug(f"failed to retrive url from {job_num=} {page_url=}")
        driver = self._driver_factory()
        try:
            self._get_url(url=page_url, driver=driver)
            # select listing
            [div_element] = driver.find_elements(
                By.CSS_SELECTOR, self._JOB_BOX_TEMPLATE.format(job_num=job_num)
            )
            [button] = div_element.find_elements(By.TAG_NAME, "button")
            button.click()
            url = driver.current_url

            # highlight and copy everything in it
            elements = driver.find_elements(By.XPATH, "//*")
            text_content = ""

            for element in elements:
                try:
                    driver.execute_script("arguments[0].style.backgroundColor = 'yellow';", element)
                    element_text = element.text

                    text_content += element_text
                except Exception as _:
                    # breakpoint()
                    pass
        except Exception as e:
            driver.quit()
            return url, str(e)

        return driver.current_url, text_content


    def _get_results_on_page(
        self, page_url: str, expected_total_jobs: int
    ) -> dict[str, str]:
        """
        Get all job listing urls from a given page url.
        """
        get_listing_text = partial(
            self._get_listing_text_highlight,
            page_url=page_url,
        )
        self._get_listing_html(job_num=1, page_url=page_url)
        results = process_map(
            get_listing_text,
            range(1, expected_total_jobs + 1),
            max_workers=self._max_workers,
        )
        return results


    def run(self):
        for page_num, expected_total_jobs in self._build_page_to_results_map().items():
            logging.info(f"Scraping page {page_num}.")
            page_url = self._base_url.replace(
                self._PAGE_SUBQUERY_TEMPLATE.format(page_num=1),
                self._PAGE_SUBQUERY_TEMPLATE.format(page_num=page_num),
            )
            for sub_results in self._get_results_on_page(
                page_url=page_url, expected_total_jobs=expected_total_jobs
            ):
                yield sub_results


if __name__ == "__main__":
    s = ScrapeMS(
        base_url="https://jobs.careers.microsoft.com/global/en/search?q=Software%20Engineer%20-principal%20-senior%20python%20-atlanta&lc=United%20States&p=Software%20Engineering&exp=Experienced%20professionals&rt=Individual%20Contributor&et=Full-Time&l=en_us&pg=1&pgSz=20&o=Relevance&flt=true",
        chromedriver_path=Path("chromedriver"),
        # max_workers=1,
        headless=False,
    )
    results = list(s.run())
    breakpoint()
