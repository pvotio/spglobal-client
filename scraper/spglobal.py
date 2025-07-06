import json
import multiprocessing
import threading
from multiprocessing.managers import SyncManager

from bs4 import BeautifulSoup

from config import logger, settings
from scraper.request import Request


class SPGlobal:

    THREAD_COUNT = settings.THREAD_COUNT
    BASE_URL = "https://www.spglobal.com/esg/scores/results?cid="

    def __init__(self):
        logger.info("Initializing SPGlobal instance")
        self.request = Request().request
        self.urls = [
            f"{self.BASE_URL}{id}"
            for id in [_["id"] for _ in json.load(open("./scraper/tickers.json", "r"))]
        ]
        self.countries = {
            _["country_name"]: _["country_iso3"]
            for _ in json.load(open("./scraper/countries.json", "r"))
        }
        self.result = {}
        self.tasks = []

    def run(self):
        logger.info("SPGlobal run started")
        self.tasks = self.urls.copy()
        logger.info("Fetched %d Tickers", len(self.urls))
        self.start_workers()
        logger.info("All processes and threads have completed")
        return dict(self.result)

    def start_workers(self):
        manager = self._start_sync_manager()
        self.tasks = manager.list(self.tasks)
        self.result = manager.dict()
        lock = manager.RLock()
        n_proc = multiprocessing.cpu_count()
        logger.info(
            "Starting %d processes × %d threads each", n_proc, self.THREAD_COUNT
        )
        processes = [
            multiprocessing.Process(
                target=self._process_target,
                name=f"Proc-{i}",
                args=(lock,),
            )
            for i in range(n_proc)
        ]

        for p in processes:
            p.start()
            logger.debug("Started %s", p.name)

        for p in processes:
            p.join()
            logger.debug("%s has finished", p.name)

    def _process_target(self, lock):
        local_threads = [
            threading.Thread(
                target=self.worker,
                name=f"{multiprocessing.current_process().name}-T{t}",
                args=(lock,),
                daemon=True,
            )
            for t in range(self.THREAD_COUNT)
        ]

        for t in local_threads:
            t.start()

        for t in local_threads:
            t.join()

    def worker(self, lock):
        thread_name = threading.current_thread().name
        logger.debug("%s: started", thread_name)

        while True:
            with lock:
                if not self.tasks:
                    logger.debug("%s: no more tasks", thread_name)
                    break
                url = self.tasks.pop(0)

            ticker = url.split("=")[-1]
            if ticker in self.result:
                logger.debug("%s: skipping duplicate ticker %s", thread_name, ticker)
                continue

            try:
                logger.debug("%s: fetching ESG scores for %s", thread_name, ticker)
                data = self.fetch_esg_scores(url)
                with lock:
                    self.result[ticker] = data
                logger.debug("%s: fetched data for %s", thread_name, ticker)
            except Exception as e:
                logger.warning(
                    "%s: unable to fetch data for %s: %s", thread_name, ticker, e
                )

    def fetch_esg_scores(self, url):
        logger.debug("Fetching ESG scores from %s", url)
        resp = self.request("GET", url)
        resp.raise_for_status()
        result = self._extract_esg_scores(resp.text)
        result["country_iso3"] = self.countries.get(result["country"], None)
        result["id"] = url.split("=")[-1]
        result["url"] = url
        return result

    def _extract_esg_scores(self, html):
        result = {}
        soup = BeautifulSoup(html, "html.parser")
        div = soup.find("div", id="company-data")
        score_env = soup.find("div", id="dimentions-score-env")
        score_soc = soup.find("div", id="dimentions-score-social")
        score_goc = soup.find("div", id="dimentions-score-govecon")
        info = {
            "name": div["data-long-name"],
            "ticker": div["data-company-ticker"].split(" ")[-1],
            "industry": div["data-industry"],
            "country": div["data-country"],
            "score": div["data-yoy-score"],
            "availability": div["data-availabilitylevel"],
            "score_env": [
                score_env["data-score"],
                score_env["data-avg"],
                score_env["data-max"],
            ],
            "score_social": [
                score_soc["data-score"],
                score_soc["data-avg"],
                score_soc["data-max"],
            ],
            "score_goveco": [
                score_goc["data-score"],
                score_goc["data-avg"],
                score_goc["data-max"],
            ],
        }

        for k, v in info.items():
            if "score_" in k:
                keys = ["", "_ind_average", "_ind_max"]
                for suffix, score in zip(keys, v):
                    result[k + suffix] = score

                continue

            result[k] = v

        return result

    @staticmethod
    def _start_sync_manager():
        m = SyncManager(address=("127.0.0.1", 0), authkey=b"spglobal")
        m.start()
        return m

    @staticmethod
    def _helper_extract_urls(html):
        urls = []
        soup = BeautifulSoup(html, "html.parser")
        divs = soup.find_all("div", class_="company-row d-flex")
        for div in divs:
            atag = div.find("a")
            urls.append("https://www.sustainalytics.com/esg-rating" + atag["data-href"])

        return urls
