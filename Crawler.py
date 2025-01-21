import time
from scrapy.spiders import CrawlSpider
import scrapy
from mysql.connector import connect
import datetime,pytz

now = datetime.datetime.now()
timezone = pytz.timezone("Asia/Kolkata")

class Crawler(CrawlSpider):
    name = "Chapter"
    allowed_domains = ["novelbin.com","lanovels.net"]
    def __init__(self,start_urls,book):
        self.bklist = start_urls
        self.book = book
        self.chapters = []
        self.conn = connect(host='ls-b81e911f5f67851fbb45b6145d8806a4849fba89.cf8sy28wm0qw.ap-south-1.rds.amazonaws.com',user='ashutosh',password='DAshut##godofsp33d',database='novel')
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
            [chapter['url'],chapter['content']] = self.change_title(chapter['title'],chapter['content'])
            self.chapters.append(chapter)
            if len(self.chapters) == 12:
                self.add_chapters(self.chapters)
                self.chapters = []
            self.bklist.remove({'ch':response.url,'num':num,'bookId':bid})
            if len(self.bklist) == 0:
                self.add_chapters(self.chapters)
                self.conn.close()
        
    def start_requests(self):
        print(self.book,":",len(self.bklist))
        while len(self.bklist) > 0:
            request = scrapy.Request(self.bklist[0]['ch'],callback=self.parse_chapters,cb_kwargs=dict(book=self.book,num=self.bklist[0]['num'],bid=self.bklist[0]['bookId']))
            yield request    
    def add_chapters(self,chapters):
        cursor = self.conn.cursor()
        sql = 'INSERT INTO chapter (url,number,title,content,likes,views,bookId) VALUES (%s,%s,%s,%s,%s,%s,%s)'
        for ch in chapters:
            val = [ch['url'],ch['number'],ch['title'],str(ch['content']).encode('utf8'),ch['likes'],ch['views'],ch['bookId']]
            cursor.execute(sql,val)
            self.conn.commit()
            if chapters.index(ch) == len(chapters)-1:
                query = 'INSERT INTO recents (bookId,url,title,addAt) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE url=VALUES(url),title=VALUES(title),addAt=VALUES(addAt)'
                cursor.execute(query,[ch['bookId'],ch['url'],ch['title'], timezone.localize(now).strftime(r'%Y-%m-%d %H:%M:%S.%ms')])
                self.conn.commit()
                if cursor.lastrowid != 0:
                    print(f"{self.book}: Successfully added to recents")
            
    def change_title(self,title:str,content):
        title = title.strip().lower().replace(", ", "-").replace("(fixed)", "").replace("(extra)", "").replace("(all chapters fixed)", "").replace("(all chapters fixed. download again)", "").replace("(all chapters fixed, redownload)", "").replace("(all chapters fixed, redownload book)", "").replace(". ", "-").replace("+", "").replace(".", "").replace(",", "-").replace('‽', "").replace('“', "-").replace('”', "").replace('*', "").replace('"', "").replace("'", "").replace("’", "").replace("?", "").replace("!", "").replace(' - ', "-").replace(" –", "-").replace(': ', "-").replace(";", " ").replace('/', "-").replace(r'\\', "-").replace('[', "-").replace(']', "").replace('(', "-").replace(')', "").replace(" ", "-").replace("--", "-")
        content = "[hereisbreak]".join(content)
        content = content.replace("â", '"').replace("Â¯", "¯").replace('"½', "").replace('ï»¿', "").replace('"¦..', "").replace('"¦.', "").replace('"¦...', "").replace('Ä±', "I").replace('"s ', "'s ").replace('"ll ', "'ll ").replace('"ve ', "'ve ").replace(' " ', " ").replace('"t ', "'t ").replace('"d ', "'d ").replace('', "").replace('', "").replace('', "").replace('', "").replace('', "").replace('"¦', "...").replace(r'Nôv(el)B\\jnn', "").replace('n/ô/vel/b//in dot c//om', "").replace('n/o/vel/b//in dot c//om', "").replace('Your next read awaits at empire', "").replace('Read exclusive adventures at empire', "").replace("Read latest stories on empire", "").replace('Discover hidden stories at empire', "").replace("Discover hidden tales at empire", "").replace('Continue your adventure with empire', "").replace('Find exclusive stories on empire', "").replace("Find your next adventure on empire", "").replace('Continue reading at empire', "").replace("Find your next read at empire", "").replace('Explore more stories with empire', "").replace("Enjoy exclusive adventures from empire", "").replace("Explore more stories at empire", "").replace(r"Nôv(el)B\\jnn", "").replace('n/o/vel/b//in dot c//om', "").replace('KÃ¶prÃ¼lÃ¼', "").replace(' n/Ã´/vel/b//jn dot c//om', "").replace("pÎ±ndÎ±,noÎ½É1,ÑoÐ .", "")
        if title.endswith("-"):
            return [title[:-1],content]
        else:
            return [title,content]

