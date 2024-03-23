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
from tqdm.contrib.concurrent import process_map
import pandas as pd
import datetime as dt
import os

logging.basicConfig(level=logging.INFO)


class ListingInfo(tp.NamedTuple):
    url: str
    error: bool
    error_msg: str
    overview: str
    qualifications: str
    responsibilities: str


class ScrapeMS:

    _JSON_ORIENTATION = "table"
    _HEADLESS = True
    _CHROMEDRIVER_PATH = Path("chromedriver")
    _WAIT_DEFAULT = 5
    _TOTAL_RESULTS_PREFIX = "Showing 1-20 of"
    _PAGE_SUBQUERY_TEMPLATE = "&pg={page_num}&"
    _JOB_BOX_TEMPLATE = "div[role='listitem'][class='ms-List-cell'][data-list-index='{listing_num}'][data-automationid='ListCell']"
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
    _FP_ROOT = "listing-microsoft"
    _FP_TEMPLATE = _FP_ROOT + "_{date_str}_{revision}.json"

    def __init__(
        self,
        listing_data: pd.DataFrame,
    ):
        self._listing_data = listing_data

    @classmethod
    def _calculate_revision(cls, output_dir: Path, date: dt.date) -> int:
        """
        Increment revision to avoid overwrites.
        """
        all_files = os.listdir(output_dir)

        matches = []
        for fp in all_files:
            if fp.startswith(cls._FP_ROOT):
                _, date_str, revision_ext = fp.split("_")
                revision, _ = revision_ext.split(".")
                if dt.date.fromisoformat(date_str) == date:
                    matches.append(revision)

        return max(matches)


    def to_disk(self, output_dir: Path) -> Path:
        today = dt.date.today()
        revision = self._calculate_revision(output_dir=output_dir, date=today)
        fp = self._FP_TEMPLATE.format(date_str=today, revision=revision)

        full_fp = output_dir / fp
        self._listing_data.to_json(full_fp, orient=self._JSON_ORIENTATION)

        return full_fp

    @classmethod
    def from_disk(cls, fp: Path):
        df = pd.read_json(fp, orient=cls._JSON_ORIENTATION)
        return cls(listing_data=df)

    @classmethod
    def from_url(
        cls,
        base_url: str,
        max_workers: int | None = None,
    ):
        ptr_map = cls._build_page_to_results_map(base_url=base_url)
        total_results = sum([res for _, (_, res) in ptr_map.items()])
        logging.info(
            f"Located {total_results} total listing(s) over {len(ptr_map)} pages."
        )
        df = cls._process_all_pages(
            base_url=base_url, max_workers=max_workers, page_to_results_map=ptr_map
        )
        cls(listing_data=df)

    @classmethod
    def _build_driver(cls) -> WebDriver:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--window-size=1600,1000")
        if cls._HEADLESS:
            chrome_options.add_argument("--headless")
        service = service = Service(str(cls._CHROMEDRIVER_PATH))
        return webdriver.Chrome(service=service, options=chrome_options)

    @classmethod
    def _get_url(cls, url: str, driver: WebDriver) -> None:
        """
        Loads a `url` onto the provided `driver` enforcing a waiting period.
        """
        driver.get(url)
        time.sleep(cls._WAIT_DEFAULT)

    @classmethod
    def _build_page_to_results_map(cls, base_url: str) -> dict[str, tuple[int, int]]:
        """
        Determine number of pages and job listings per page.
        """
        driver = cls._build_driver()
        cls._get_url(base_url, driver=driver)

        h1_element = driver.find_element(
            By.XPATH, f"//h1[contains(., '{cls._TOTAL_RESULTS_PREFIX}')]"
        )
        _, page_size, total_results = map(int, re.findall(r"\d+", h1_element.text))

        total_pages = math.ceil(total_results / page_size)
        last_page_total = total_results % page_size

        page_to_results_map = {
            cls._build_page_url(base_url=base_url, page_num=page): (page, page_size)
            for page in range(1, total_pages)
        }
        page_to_results_map[
            cls._build_page_url(base_url=base_url, page_num=last_page_total)
        ] = (last_page_total, total_pages + 1)

        return page_to_results_map

    @classmethod
    def _select_listing(cls, driver: WebDriver, listing_num: int) -> None:
        [div_element] = driver.find_elements(
            By.CSS_SELECTOR, cls._JOB_BOX_TEMPLATE.format(listing_num=listing_num)
        )
        [button] = div_element.find_elements(By.TAG_NAME, "button")
        button.click()
        time.sleep(cls._WAIT_DEFAULT)

    @classmethod
    def _retrieve_listing_info(
        cls,
        driver: WebDriver,
    ) -> ListingInfo:

        first_horizontal_line, *_ = driver.find_elements(
            By.CSS_SELECTOR, "hr[class^='horizontalLine-']"
        )
        next_div, *_ = first_horizontal_line.find_elements(
            By.XPATH, "following-sibling::div"
        )
        overview, qualifications, responsibilities, *_ = next_div.find_elements(
            By.XPATH, "./div"
        )

        li = ListingInfo(
            url=driver.current_url,
            error=False,
            error_msg="",
            overview=overview.text,
            qualifications=qualifications.text,
            responsibilities=responsibilities.text,
        )

        return li

    @classmethod
    def _process_page(
        cls, page_url: str, listings_on_page: int
    ) -> tp.List[ListingInfo]:
        driver = cls._build_driver()
        cls._get_url(page_url, driver=driver)

        results = []
        for listing_num in range(1, listings_on_page + 1):
            cls._select_listing(driver=driver, listing_num=listing_num)
            li = cls._retrieve_listing_info(driver=driver)
            results.append(li)

        return results

    # TODO figure out how to get rid of this
    @classmethod
    def _process_page_from_tuple(
        cls, url_listings: tuple[str, int, int]
    ) -> tp.List[ListingInfo]:
        page_url, page, total = url_listings

        driver = cls._build_driver()
        cls._get_url(page_url, driver=driver)

        results = []
        for listing_num in range(1, total + 1):
            try:
                cls._select_listing(driver=driver, listing_num=listing_num)
                li = cls._retrieve_listing_info(driver=driver)
            except Exception as e:
                li = ListingInfo(
                    url=driver.current_url,
                    error=False,
                    error_msg=str(e),
                    overview="",
                    qualifications="",
                    responsibilities="",
                )
            results.append(li)
        return results

    @classmethod
    def _build_page_url(cls, base_url: str, page_num: int) -> str:
        page_url = base_url.replace(
            cls._PAGE_SUBQUERY_TEMPLATE.format(page_num=1),
            cls._PAGE_SUBQUERY_TEMPLATE.format(page_num=page_num),
        )
        return page_url

    @classmethod
    def _process_all_pages(
        cls,
        base_url: str,
        max_workers: int | None,
        page_to_results_map: dict[str, tuple[int, int]],
    ) -> pd.DataFrame:
        inputs = [(url, page, total) for url, (page, total) in page_to_results_map.items()]
        results_raw = process_map(
            cls._process_page_from_tuple, inputs, max_workers=max_workers
        )
        results = [r for res in results_raw for r in res]
        df = pd.DataFrame(results._asdict() for results in results)

        return df


if __name__ == "__main__":
    # s = ScrapeMS.from_url(
    #     base_url="https://jobs.careers.microsoft.com/global/en/search?q=Software%20Engineer%20-principal%20-senior%20python%20-atlanta&lc=United%20States&p=Software%20Engineering&exp=Experienced%20professionals&rt=Individual%20Contributor&et=Full-Time&l=en_us&pg=1&pgSz=20&o=Relevance&flt=true"
    # )
    s = ScrapeMS.from_disk(Path("listings-microsoft_2024-03-21_0001.json"))
    breakpoint()
