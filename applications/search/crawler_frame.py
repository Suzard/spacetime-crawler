import logging
from datamodel.search.SkayaniEdwardc6Forsterj_datamodel import SkayaniEdwardc6ForsterjLink, \
    OneSkayaniEdwardc6ForsterjUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
# from lxml import html,etree
# from lxml.html import fromstring
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs
from uuid import uuid4

# My Imports
import bs4 as bs
from urlparse import urljoin

# My Imports End

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

# My Global
links_processed = set()
subdomains_visited = {}
mostOutLinks_url = ""
mostOutLinks_total = -1
links_cap = 100


# My Global End

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
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''
    outputLinks = []
    global mostOutLinks_total
    global mostOutLinks_url

    # print("RawDataObj URL: " + rawDataObj.url.encode('utf-8'))
    # print("RawDataObj content type: ", type(rawDataObj.content))
    # print("RawDataObj error msg: " + str(rawDataObj.error_message))

    if (rawDataObj.http_code > 399):  # Contains error code
        return outputLinks

    soup = bs.BeautifulSoup(rawDataObj.content.decode('utf-8'), 'lxml')

    for tagObj in soup.find_all('a'):
        if (tagObj.attrs.has_key('href')):
            # print(tagObj['href'].encode('utf-8'))
            outputLinks.append(urljoin(rawDataObj.url.decode('utf-8'), tagObj['href']).encode('utf-8'))

    if (len(outputLinks) > mostOutLinks_total):
        mostOutLinks_total = len(outputLinks)
        mostOutLinks_url = rawDataObj.url
    return outputLinks


def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    # print("is_valid in URL: " + url)
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        # Ignore non-ics.uci.edu; Ignore queries; Ignore Calendar
        if (".ics.uci.edu" in parsed.hostname) \
                and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                 + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                 + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                 + "|thmx|mso|arff|rtf|jar|csv" \
                                 + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()) \
                and len(parsed.query) == 0 \
                and not re.match("^calendar.*", parsed.path.lower()):

            if (not subdomains_visited.has_key(parsed.netloc)):
                subdomains_visited[parsed.netloc] = set()
            subdomains_visited[parsed.netloc].add(url)
            links_processed.add(url)
            if (len(links_processed) > links_cap):
                print("Done")
                for key, value in subdomains_visited.items():
                    print(type(key))
                    print(type(value))
                    print("Subdomain: "+key)
                    print("Subdomain URLS: "+str(len(value)))
                print("Page with Most Links: "+mostOutLinks_url)
                print("Total: "+str(mostOutLinks_total))
                raise KeyboardInterrupt

            return True
        else:
            return False


    except TypeError:
        print ("TypeError for ", parsed)
        return False
