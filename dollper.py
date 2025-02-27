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
done_file = "done.txt"


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
        logging.debug("Fetching image page %s", self.page_url)
        html = await get_html(self.page_url, self.session)

        logging.debug("Parsing image from image page %s", self.page_url)
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

        logging.debug("Fetching set %s", set_page_url)
        html = await get_html(set_page_url, self.session)

        logging.debug("Parsing set for images %s", set_page_url)
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
                logging.debug("Set has next page %s -> %s", set_page_url, next_page_url)
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
        logging.debug("Fetching page %d", self.id)
        html = await get_html(self.url, self.session)

        logging.debug("Parsing sets from page %d", self.id)
        soup = BeautifulSoup(html, "html.parser")
        matches = soup.find_all(class_="picbox")

        if not matches:
            logging.debug("No sets found %s", self.url)
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
        logging.debug("Output folder doesn't exist %s", dest)
        return

    if not image.image_url:
        logging.debug("Image has no URL")
        return

    filename = f"{dest}/{image.id}.{image.image_url.split('.')[-1]}"

    if os.path.isfile(filename):
        logging.debug("Image already exists (skipping) %s", filename)
        return

    logging.debug("Downloading %s", filename)
    with async_timeout.timeout(120):
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

    info_file = to_folder + "/info.txt"
    if os.path.isfile(info_file):
        logging.debug("Set already complete %s", s.title)
        return

    pool = 20
    tmp = s.images.copy()
    while True:
        await asyncio.gather(*(download_image(image, to_folder, session) for image in tmp[:pool]))
        tmp = tmp[pool:]
        if not tmp:
            break

    with open(info_file, "w") as f:
        f.write(f"Set url: {s.url}")


async def download_page(page, session):
    # await asyncio.gather(*(download_set(s, session) for s in page.sets))
    logging.info("Downloading page %d", page.id)

    for s in page.sets:
        await download_set(s, session)
    with open(done_file, "a+") as f:
        f.write(str(page.id) + "\n")


async def main():
    last_page = 1617
    pages_to_fetch = list(range(1, last_page + 1))
    if os.path.isfile(done_file):
        with open(done_file) as f:
            pages_done = [int(x) for x in f.read().splitlines()]
            pages_to_fetch = [x for x in pages_to_fetch if x not in pages_done]
            pages_to_fetch.sort()

    async with ClientSession() as session:
        pages = [Page(id=i, session=session) for i in pages_to_fetch]
        for page in pages:
            await page.parse()
            await download_page(page, session)


if __name__ == '__main__':
    start = time.time()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    logging.debug("Elapsed time %fs", (time.time() - start))
