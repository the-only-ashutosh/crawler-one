import aiohttp
import asyncio
import time
from bs4 import BeautifulSoup
import requests, math,os
from lxml import etree
from PIL import Image
import boto3
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
import Crawler as ChapterCrawler
from twisted.internet import defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from mysql.connector import connect


s3 = boto3.client('s3',region_name='ap-south-1a',endpoint_url='https://bucket-tkgyhr.s3.ap-south-1.amazonaws.com',aws_access_key_id='',aws_secret_access_key='') 
options = webdriver.ChromeOptions()
# options.add_argument("start-maximized")
options.add_argument("headless")
    #chrome to stay open
# options.add_experimental_option("detach", True)
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
def image_to_base64(url,name):
    response = requests.get(url)
    if response.status_code == 200:
         f = open(f'{name}.jpg','wb') 
         f.write(response.content) 
         f.close() 
         im = Image.open(f'{name}.jpg').convert("RGB")
         im.save(f"{name}.webp","webp")
         width = im.width
         height = im.height
        #  t = 0
         s3.upload_file(f"{name}.webp",Bucket='nz', Key=f"{name}.webp")
        #  while t == 0:
        #      time.sleep(1)
         os.remove(f"{name}.jpg")
         os.remove(f"{name}.webp")
         return width/height
    else:
         return "Don't add."

mydomain = "https://novelzone.fun"
conn = connect(host='ls-b81e911f5f67851fbb45b6145d8806a4849fba89.cf8sy28wm0qw.ap-south-1.rds.amazonaws.com',user='ashutosh',password='DAshut##godofsp33d',database='novel')
cursor = conn.cursor(buffered=True)
settings={
    "LOG_LEVEL":"INFO",
    # "LOG_ENABLED":False,
    'RETRY_ENABLED': True,
    'HTTPERROR_ALLOWED_CODES': [500, 502, 503, 504, 408, 429, 400,521]
}
configure_logging(settings)
runner = CrawlerRunner(settings)
processes = []
async def main(): 
    urll = "https://novelbin.com/sort/latest"
    # urll = "https://novelbin.com/sort/completed"    
    pa = "Page:"
    for i in range(1,2).__reversed__():
        u = f'{urll}?page={i}'
        async with aiohttp.ClientSession() as session:
            content = await fetch_page(session,u)
            print(pa,i,"Crawled")
        books = []
        for one in content.find_all('div',class_='col-xs-7'):
            url = one.find('h3',class_='novel-title').find('a')['href']
            ishot = one.find('span',class_='label-hot') is not None
            books.append({'url':url, 'ishot':ishot})
        #Sorting books
        avbooks = []
        unavbooks = []
        for book in books:
            bookurl = str(book['url'].split('/b/')[1])
            query = "SELECT id FROM book WHERE bookURL=%s"
            cursor.execute(query,[bookurl])
            ids = cursor.fetchone()
            if ids is not None:
                avbooks.append({'url':book['url'],'bookId':ids[0]})
            else:
                unavbooks.append(book)
        print(pa,i,"books sorted")
        async with aiohttp.ClientSession() as session:
            unavbooks_tasks = []
            for bookurl in unavbooks:
                unavbooks_tasks.append(fetch_page(session,bookurl['url']))
            unavbooks_htmls = await asyncio.gather(*unavbooks_tasks)
        #Add Unavailable Books to Db
        for ufg, soup in zip(unavbooks, unavbooks_htmls):
            bk = {}
            page = etree.HTML(str(soup))
            bk['bookUrl'] = ufg['url'].split('/b/')[1]
            bk['title'] = page.xpath("//div[@class='desc']//h3[@class='title']/text()")[0]
            imageurl = page.xpath(f'//img[contains(@alt,"{bk["title"]}")]/@data-src')[0]
            bk['imageUrl'] = f"https://img.novelzone.fun/nz/{bk['bookUrl']}.webp"
            bk['author'] = str(page.xpath("//ul[@class='info info-meta']/li[h3='Author:']/a/text()")[0]).split("|")[0].strip()
            bk['genre'] = page.xpath("//ul[@class='info info-meta']/li[h3='Genre:']/a/text()")
            bk['status'] = page.xpath("//ul[@class='info info-meta']/li[h3='Status:']/a/text()")[0]
            bk['categories'] = page.xpath("//ul[@class='info info-meta']/li/div[@class='tag-container']/a/text()")
            bk['description'] = page.xpath("//div[@class='desc-text']/p/text()")
            bk['isHot'] = ufg['ishot']
            bk['userrated'] = int(page.xpath("//div[@class='rate-info']/div[@class='small']/em/strong/span[contains(@itemprop,'reviewCount')]/text()")[0])
            bk['totalStars'] = math.ceil(bk['userrated']*float(page.xpath("//div[@class='rate-info']/div[@class='small']/em/strong/span[contains(@itemprop,'ratingValue')]/text()")[0]))/2
            bk['views'] = 0
            if len(bk['description']) == 0:
                bk['description'] = [page.xpath("//div[@class='desc-text']/text()")[0]]
            if imageurl != None:
                bk['aspectRatio'] = image_to_base64(imageurl,bk["bookUrl"])
            if bk['aspectRatio'] == "Don't add.":
                continue   
            bk['authId'] = requests.post(f"{mydomain}/api/myauth/addAuthor",json={"author":bk['author']}).json()
            bookid = requests.post(f"{mydomain}/api/myauth/addBook",json=bk).json()
            print(bk['title'],":",bookid)
            if bookid is not None:
                avbooks.append({'url':ufg['url'],'bookId':bookid})
        print("Unavailable books added!")
        for x in range(len(avbooks)):
            book = avbooks[x]['url'].split('/b/')[1]
            soup = await fetch_chapters(avbooks[x]['url'])
            chaplist = []
            vipstart = False
            for sgul in soup.find_all('ul',class_='list-chapter'):
                for sgli in sgul.find_all('li'):
                    if vipstart:
                        continue
                    sgli = etree.HTML(str(sgli))
                    isvip = len(sgli.xpath(".//a/span/span[@class='premium-label']")) != 0
                    if not isvip:
                        isvip = len(sgli.xpath(".//a/span/span[@class='vip-label']")) != 0
                    chapter = str(sgli.xpath(".//a/@href")[0]).replace("?subsite=1","")
                    if isvip:
                        vipstart = True
                    if not isvip:
                        chaplist.append({'ch':chapter,'num':len(chaplist)+1,'bookId':avbooks[x]['bookId']})
            if len(chaplist) == 30 or len(chaplist) == 0:
                continue        
            print(pa,i,"-",book,"chapters sorted")
            try:
                chaplis = requests.post(f"{mydomain}/api/myauth/checkChapterAvailable",json={"book":book,"chapter":chaplist}).json()['chapters']
            except requests.exceptions.JSONDecodeError:
                continue
            print(len(chaplist),len(chaplis))
            if len(chaplis) >0:
                processes.append({'crawler':ChapterCrawler.Crawler,'start_urls':chaplis,'book':book})
            
        
async def fetch_page(session, url):
    # make GET request using session
    async with session.get(url) as response:
        # return HTML content
        html_content = await response.text()
        # parse HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        # return parsed HTML
        return soup

async def fetch_chapters(url):
    driver.get(url+"#tab-chapters-title")
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source,'html.parser')
    return soup    

from twisted.internet import reactor
@defer.inlineCallbacks
def crawl():
    for p in processes:
        yield runner.crawl(p['crawler'], start_urls=p['start_urls'],book=p['book'])
    reactor.stop()


asyncio.run(main())
crawl()
reactor.run()           