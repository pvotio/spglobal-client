import random
import time

import requests

from config import logger, settings


class Request:

    def __init__(self):
        self.useragents = [
            ua.strip() for ua in open("./scraper/useragents.txt", "r").readlines()
        ]

    def request(self, method: str, url: str, *args, **kwargs) -> requests.Request:
        for i in range(settings.REQUEST_MAX_RETRIES):
            try:
                if not kwargs.get("proxies", None):
                    kwargs["proxies"] = self.__get_proxy()

                if not kwargs.get("headers", None):
                    kwargs["headers"] = {"User-Agent": random.choice(self.useragents)}
                else:
                    kwargs["headers"]["User-Agent"] = random.choice(self.useragents)

                req = requests.request(method=method, url=url, *args, **kwargs)
                req.raise_for_status()
                return req
            except requests.exceptions.RequestException as e:
                logger.debug(
                    f"{method.upper()} request failed. Attempt {i + 1} of {settings.REQUEST_MAX_RETRIES}. Error: {e}"  # noqa: E501
                )
                if (i + 1) == settings.REQUEST_MAX_RETRIES:
                    raise

                time.sleep(i**settings.REQUEST_BACKOFF_FACTOR)

    def __get_proxy(self):
        proxy_creds = f"{settings.BRIGHTDATA_USER}-session-{random.random()}:{settings.BRIGHTDATA_PASSWD}"  # noqa: E501
        proxies = {
            "http": f"http://{proxy_creds}@{settings.BRIGHTDATA_PROXY}:{settings.BRIGHTDATA_PORT}",  # noqa: E501
            "https": f"https://{proxy_creds}@{settings.BRIGHTDATA_PROXY}:{settings.BRIGHTDATA_PORT}",  # noqa: E501
        }
        return proxies
