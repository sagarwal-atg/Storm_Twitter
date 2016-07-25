import storm
import urllib2
from bs4 import BeautifulSoup



class UrlTextBolt(storm.BasicBolt):
    def process(self, tup):
        #TO DO: Add check for empty values
        url = tup.values[0]
        try:
            html = urllib2.urlopen(url).read()
            soup = BeautifulSoup(html)
            urlText = soup.findAll({'title' : True, 'p' : True})
            if urlText:
                for url_t in urlText:
                    storm.emit([url_t.string])
        except:
            pass

UrlTextBolt().run()
