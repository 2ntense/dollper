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

logging.basicConfig(level=logging.INFO)
root_url = private.root_url_


async def get_html(url, session):
    async with session.get(url) as resp:
        html = await resp.text()
        return html


@dataclass
class Image:
    session: ClientSession
    page_url: str
    image_url: str = None
    id: int = None

    async def parse_image(self):
        logging.info("Fetching image page %s", self.page_url)
        html = await get_html(self.page_url, self.session)

        logging.info("Parsing image from image page %s", self.page_url)
        soup = BeautifulSoup(html, "html.parser")

        match = soup.find(class_=private.class_)
        if not match:
            return

        self.image_url = match.img["src"]

        match = soup.find(class_="imgpagebar")
        if not match:
            return

        self.id = match.h2.text.split("|")[0].strip()


@dataclass
class Set:
    title: str
    url: str  # first page
    session: ClientSession
    images: List[Image] = field(default_factory=list)

    async def parse_image_pages(self, set_page_url=None):
        if set_page_url is None:
            set_page_url = self.url

        logging.info("Fetching set %s", set_page_url)
        html = await get_html(set_page_url, self.session)

        logging.info("Parsing set for images %s", set_page_url)
        soup = BeautifulSoup(html, "html.parser")

        matches = soup.find_all(class_="image")
        if not matches:
            return

        for m in matches:
            image_page_url = root_url + m.a["href"]
            self.images.append(Image(page_url=image_page_url, session=self.session))

        nav_bar = soup.find(class_="imgpagebar")
        if not nav_bar:
            return

        for row in nav_bar:
            if "Next Page" in str(row):
                next_page_url = root_url + row["href"]
                logging.info("Set has next page %s -> %s", set_page_url, next_page_url)
                await self.parse_image_pages(set_page_url=next_page_url)

    async def parse_images(self):
        if not self.images:
            return
        await asyncio.gather(*(image.parse_image() for image in self.images))


@dataclass
class Page:
    id: int
    session: ClientSession
    url: str = None
    sets: List[Set] = field(default_factory=list)

    def __post_init__(self):
        self.url = f"{root_url}/page-{self.id}.html"

    async def parse_sets(self):
        logging.info("Fetching page %d", self.id)
        html = await get_html(self.url, self.session)

        logging.info("Parsing sets from page %d", self.id)
        soup = BeautifulSoup(html, "html.parser")
        matches = soup.find_all(class_="picbox")

        if not matches:
            logging.info("No sets found %s", self.url)
            return

        for m in matches:
            title = m.h4.text.strip()
            url = root_url + m.h4.a["href"]
            self.sets.append(Set(title=title, url=url, session=self.session))

    async def parse(self):
        await self.parse_sets()
        await asyncio.gather(*(s.parse_image_pages() for s in self.sets))
        for s in self.sets:
            await asyncio.gather(*(image.parse_image() for image in s.images))


async def download_image(image: Image, dest: str, session):
    if not os.path.isdir(dest):
        logging.info("Output folder doesn't exist %s", dest)
        return

    if not image.image_url:
        logging.info("Image has no URL")
        return

    filename = f"{dest}/{image.id}.{image.image_url.split('.')[-1]}"

    if os.path.isfile(filename):
        logging.info("Image already exists (skipping) %s", filename)
        return

    logging.info("Downloading %s", filename)
    with async_timeout.timeout(10):
        async with session.get(image.image_url) as resp:
            async with aiofiles.open(filename, 'wb') as fd:
                while True:
                    chunk = await resp.content.read(1024)
                    if not chunk:
                        break
                    await fd.write(chunk)
            return await resp.release()


async def download_set(s: Set, session):
    to_folder = f"./dl/{s.title}"
    os.makedirs(to_folder, exist_ok=True)

    if os.path.isfile(to_folder + "/info.txt"):
        logging.info("Set already complete %s", s.title)
        return

    # TODO
    # for image in s.images:
    #     await download_image(image, to_folder, session)
    await asyncio.gather(*(download_image(image, to_folder, session) for image in s.images))
    with open("info.txt", "w") as f:
        f.write(f"Set url: {s.url}")


async def download_page(page, session):
    # await asyncio.gather(*(download_set(s, session) for s in page.sets))
    for s in page.sets:
        await download_set(s, session)
    with open("done.txt", "a+") as f:
        f.write(str(page.id) + "\n")


async def main():
    save_file = "done.txt"
    last_page = 1617
    pages_to_fetch = list(range(1, last_page + 1))
    if os.path.isfile(save_file):
        with open(save_file) as f:
            pages_done = [int(x) for x in f.read().splitlines()]
            pages_to_fetch = [x for x in pages_to_fetch if x not in pages_done]
            pages_to_fetch.sort()

    async with ClientSession() as session:
        pages = [Page(id=i, session=session) for i in pages_to_fetch]
        for page in pages:
            await page.parse()
            await download_page(page, session)

    # async with ClientSession() as session:
    #
    #     pages = [Page(id=i, session=session) for i in pages_to_fetch]
    #
    #     pool = 2
    #     b = 0
    #     e = pool
    #
    #     if e <= len(pages):
    #         while True:
    #             pages_ = pages[b:e]
    #             await asyncio.gather(*(page.parse() for page in pages[b:e]))
    #             await asyncio.gather(*(download_page(page, session) for page in pages[b:e]))
    #             b += pool
    #             e += pool
    #             if e > len(pages):
    #                 break
    #     else:
    #         await asyncio.gather(*(page.parse() for page in pages))
    #         logging.info("Begin download of images")
    #         await asyncio.gather(*(download_page(page, session) for page in pages))


if __name__ == '__main__':
    start = time.time()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    logging.info("Elapsed time %fs", (time.time() - start))
