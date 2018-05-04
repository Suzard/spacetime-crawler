import logging
from datamodel.search.SkayaniEdwardc6Forsterj_datamodel import SkayaniEdwardc6ForsterjLink, OneSkayaniEdwardc6ForsterjUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
#from lxml import html,etree
#from lxml.html import fromstring
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs, urljoin
from uuid import uuid4

# My imports
import bs4 as bs
from urllib2 import urlopen
import re

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

links_processed_dict = {}
total_links_processed = 0
page_with_max_outlinks = ""
links_cap = 3000

@Producer(SkayaniEdwardc6ForsterjLink)
@GetterSetter(OneSkayaniEdwardc6ForsterjUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "SkayaniEdwardc6Forsterj"

    def __init__(self, frame):
        self.app_id = "SkayaniEdwardc6Forsterj"
        self.frame = frame

    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneSkayaniEdwardc6ForsterjUnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = SkayaniEdwardc6ForsterjLink("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneSkayaniEdwardc6ForsterjUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(SkayaniEdwardc6ForsterjLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    # print("RawDataObj URL: " + str(rawDataObj.url))
    # print("RawDataObj content type: " + rawDataObj.content)
    # print("RawDataObj error msg: " + str(rawDataObj.error_message))

    sauce = urlopen(rawDataObj.url).read()
    soup = bs.BeautifulSoup(sauce, 'lxml')


    try:
		script_tags = soup.find('script')
		print(script_tags)
		for url in soup.find_all('a'):
			url['href'] = urljoin(rawDataObj.url, url['href'])
			if ("mailto" not in url['href'] and url.get('script') == None):
				outputLinks.append(url['href'])

    except Exception as e:
		print(e)

    print(outputLinks[0:20])
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    print("is_valid in URL: " + url)
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]) or "php" in url:
        return False
    try:
        if ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()):
            
            global total_links_processed
            global links_cap
            if (total_links_processed >= links_cap):
            	print("Reached limit!!!!!!!!!!!!!!")
            total_links_processed += 1
            print(total_links_processed)
            return True


    except TypeError:
        print ("TypeError for ", parsed)
        return False
