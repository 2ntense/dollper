from concurrent.futures import FIRST_COMPLETED

import private

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import List

import aiofiles
import async_timeout
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from concurrent import futures
import requests

logging.basicConfig(level=logging.DEBUG)
root_url = private.root_url_
done_file = "done.txt"

chunk_size = 1024
BASE_URL = 'http://flupy.org/data/flags'
POP20_CC = ('CN IN US ID BR PK NG BD RU JP '
            'MX PH VN ET EG DE IR TR CD FR').split()


# async def get_html(url, session):
#     async with session.get(url) as resp:
#         html = await resp.text()
#         return html


def retrieve_html(url):
    with requests.get(url) as r:
        return r.text


def retrieve_file(cc: str, dest_file: str):
    url = '{}/{cc}/{cc}.gif'.format(BASE_URL, cc=cc.lower())

    logging.debug("Retrieving file %s -> %s", url, dest_file)
    with requests.get(url, stream=True) as r:
        with open(dest_file, "wb") as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    fd.write(chunk)
        logging.debug("File retrieved %s -> %s", url, dest_file)


@dataclass
class ImageX:
    page_url: str
    _image_url: str = None
    _id: int = None

    @property
    def _html(self):
        return retrieve_html(self.page_url)

    @property
    def id(self):
        if not self._id:
            self.parse()
        return self._id

    @property
    def image_url(self):
        if not self._image_url:
            self.parse()
        return self._image_url

    def parse(self):
        soup = BeautifulSoup(self._html, "html.parser")

        match = soup.find(class_=private.class_)
        if not match:
            return
        self._image_url = match.img["src"]

        match = soup.find(class_="imgpagebar")
        if not match:
            return
        self._id = match.h2.text.split("|")[0].strip()


@dataclass
class SetX:
    title: str
    url: str  # first page
    _images: list = field(default_factory=list)

    @property
    def _html(self):
        return retrieve_html(self.url)

    @property
    def images(self):
        if not self._images:
            self.parse_images()
        return self._images

    def parse_images(self):
        def _get_next_page_url(s: BeautifulSoup):
            nav_bar = s.find(class_="imgpagebar")
            if not nav_bar:
                return None
            for row in nav_bar:
                if "Next Page" in str(row):
                    next_page_url = root_url + row["href"]
                    return next_page_url

        def _parse_page(s: BeautifulSoup):
            matches = s.find_all(class_="image")
            if not matches:
                return
            for m in matches:
                image_page_url = root_url + m.a["href"]
                self._images.append(ImageX(page_url=image_page_url))

        with futures.ThreadPoolExecutor(20) as executor:
            url = self.url
            while url:
                soup = BeautifulSoup(retrieve_html(url), "html.parser")
                executor.submit(_parse_page, soup)
                url = _get_next_page_url(soup)


@dataclass
class PageX:
    no: int
    _sets: list = field(default_factory=list)

    @property
    def url(self):
        return f"{root_url}/page-{self.no}.html"

    @property
    def _html(self):
        logging.debug("Fetching page %d", self.no)
        return retrieve_html(self.url)

    @property
    def sets(self):
        if not self._sets:
            self.parse_sets()
        return self._sets

    def parse_sets(self):
        soup = BeautifulSoup(self._html, "html.parser")
        matches = soup.find_all(class_="picbox")

        if not matches:
            logging.debug("No sets found %s", self.url)
            return

        for m in matches:
            title = m.h4.text.strip()
            url = root_url + m.h4.a["href"]
            self._sets.append(SetX(title=title, url=url))


# async def download_image(image: Image, dest: str, session):
#     if not os.path.isdir(dest):
#         logging.debug("Output folder doesn't exist %s", dest)
#         return
#
#     if not image.image_url:
#         logging.debug("Image has no URL")
#         return
#
#     filename = f"{dest}/{image.id}.{image.image_url.split('.')[-1]}"
#
#     if os.path.isfile(filename):
#         logging.debug("Image already exists (skipping) %s", filename)
#         return
#
#     logging.debug("Downloading %s", filename)
#     with async_timeout.timeout(120):
#         async with session.get(image.image_url) as resp:
#             async with aiofiles.open(filename, 'wb') as fd:
#                 while True:
#                     chunk = await resp.content.read(1024)
#                     if not chunk:
#                         break
#                     await fd.write(chunk)
#             return await resp.release()
#
#
# async def download_set(s: Set, session):
#     to_folder = f"./dl/{s.title}"
#     os.makedirs(to_folder, exist_ok=True)
#
#     info_file = to_folder + "/info.txt"
#     if os.path.isfile(info_file):
#         logging.debug("Set already complete %s", s.title)
#         return
#
#     pool = 20
#     tmp = s.images.copy()
#     while True:
#         await asyncio.gather(*(download_image(image, to_folder, session) for image in tmp[:pool]))
#         tmp = tmp[pool:]
#         if not tmp:
#             break
#
#     with open(info_file, "w") as f:
#         f.write(f"Set url: {s.url}")
#
#
# async def download_page(page, session):
#     # await asyncio.gather(*(download_set(s, session) for s in page.sets))
#     logging.info("Downloading page %d", page.id)
#
#     for s in page.sets:
#         await download_set(s, session)
#     with open(done_file, "a+") as f:
#         f.write(str(page.id) + "\n")
#
#
# async def main():
#     last_page = 1617
#     pages_to_fetch = list(range(1, last_page + 1))
#     if os.path.isfile(done_file):
#         with open(done_file) as f:
#             pages_done = [int(x) for x in f.read().splitlines()]
#             pages_to_fetch = [x for x in pages_to_fetch if x not in pages_done]
#             pages_to_fetch.sort()
#
#     async with ClientSession() as session:
#         pages = [Page(id=i, session=session) for i in pages_to_fetch]
#         for page in pages:
#             await page.parse()
#             await download_page(page, session)


def parse_page(page: PageX, everything=False):
    if not everything:
        page.parse_sets()
    else:
        for s in page.sets:
            for i in s.images:
                i.parse()


def main2():
    last_page = 1
    pages_to_fetch = range(1, last_page + 1)
    pages = [PageX(no=no) for no in pages_to_fetch]

    tasks = []

    with futures.ThreadPoolExecutor(1) as executor:
        for page in pages:
            ftr = executor.submit(parse_page, page, everything=True)
            tasks.append(ftr)

        for ftr in futures.as_completed(tasks):
            tasks.remove(ftr)

        if len(tasks) == 0:
            print("all done")

        for page in pages:
            # for s in page.sets:
            #     print(s)
            for i in page.sets[0].images:
                print(i)
            # print(page.sets[0].images)


if __name__ == '__main__':
    start = time.time()

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())

    # retrieve_file("https://cdn.vox-cdn.com/uploads/chorus_asset/file/18942069/6400_2.jpg", "")

    main2()

    logging.debug("Elapsed time %fs", (time.time() - start))
