import time
from scrapy.spiders import CrawlSpider
import scrapy,requests

class Crawler(CrawlSpider):
    name = "Chapter_Crawler"
    mydomain = "http://localhost:3000"
    # mydomain = "https://novelzone.fun"
    allowed_domains = ["novelbin.com","lanovels.net"]
    def __init__(self,start_urls,book):
        self.bklist = start_urls
        self.book = book
        self.chapters = []
    def parse_chapters(self,response,book,num,bid):
        if response.status in [500, 502, 503, 504, 408, 429, 400,521]:
            time.sleep(0.05)
        else:        
            chapter = {}
            chapter["url"] = str(response.url).split(f"/{book}/")[1].replace("?subsite=1","").split("-href")[0]
            if len(chapter['url']) > 200:
                chapter['url'] = chapter['url'][:100]
            chapter['title'] = str(response.xpath("//a[@class='chr-title']/span/text()").get()).split('" href')[0]
            if len(chapter['title']) > 200:
                chapter['title'] = chapter['title'][:100]
            chapter['number'] = num
            chapter['content'] = response.xpath("//div[@id='chr-content']/p/text()").getall()
            chapter['likes'] = 0
            chapter['views'] = 0
            chapter['bookId'] = bid
            self.chapters.append(chapter)
            try:
                if len(self.chapters) == 12:
                    res = requests.post(f"{self.mydomain}/api/myauth/addChapter",json={'chapters':self.chapters[:10]}).json()
                    print(book,":",res['message'])
                    self.chapters = self.chapters[12:]
            except requests.exceptions.JSONDecodeError:
                    pass
            self.bklist.remove({'ch':response.url,'num':num,'bookId':bid})
            if len(self.bklist) == 0:
                print(len(self.bklist),len(self.chapters))
                res = requests.post(f"{self.mydomain}/api/myauth/addChapter",json={'chapters':self.chapters}).json()
                print(book,":",res['message'])
        
    def start_requests(self):
        print(self.book,":",len(self.bklist))
        while len(self.bklist) > 0:
            request = scrapy.Request(self.bklist[0]['ch'],callback=self.parse_chapters,cb_kwargs=dict(book=self.book,num=self.bklist[0]['num'],bid=self.bklist[0]['bookId']))
            yield request     