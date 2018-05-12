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
#testing
logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
# My Global
links_processed = set()
subdomains_visited = {}
mostOutLinks_url = ""
mostOutLinks_total = -1
links_cap = 3000  # Max amount of downloaded pages before exit
query_cap = 10 # Max amount of times a url's query section will be added to the frontier
url_query_count = {}
# My Global End

@Producer(SkayaniEdwardc6ForsterjLink)
@GetterSetter(OneSkayaniEdwardc6ForsterjUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "SkayaniEdwardc6Forsterj"

    def __init__(self, frame):
        self.app_id = "SkayaniEdwardc6Forsterj"
        self.frame = frame
        self.start_time = time()

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
        global mostOutLinks_url, mostOutLinks_total, links_processed
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            # Added Code
            links_processed.add( link.full_url )
            if (not subdomains_visited.has_key(link.domain)):
                subdomains_visited[link.domain] = set()
            valid_links = 0
            # Added Code End
            for l in links:
                if is_valid(l):
                    valid_links += 1  # Added Code
                    subdomains_visited[link.domain].add(l)  # Added Code
                    self.frame.add(SkayaniEdwardc6ForsterjLink(l))
            # Added Code
            if ( valid_links > mostOutLinks_total ):
                mostOutLinks_total = valid_links
                mostOutLinks_url = link.full_url
            if ( len(links_processed) > links_cap ):
                self.shutdown()
                raise KeyboardInterrupt
            # Added Code End

    def shutdown(self):
        # All Added
        output_file = open("Analytics.txt", "w")
        print("Finished!")
        output_file.write("Analytics:\n")
        output_file.write("--------------------------------------\n")
        print (
            "Time spent this session: ",
            time() - self.start_time, " seconds.")
        for key, value in sorted(subdomains_visited.items()):
            if ( len(value) == 0):
                continue
            output_file.write("Subdomain: " + key + "\n")
            output_file.write("Subdomain URLS: " + str(len(value)) + "\n")
        output_file.write("--------------------------------------\n")
        output_file.write("Page with Most Links: " + mostOutLinks_url+"\n")
        output_file.write("Total: " + str(mostOutLinks_total)+"\n")
        output_file.close()

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

    # print("RawDataObj URL: " + rawDataObj.url.encode('utf-8'))
    # print("RawDataObj content type: ", type(rawDataObj.content))
    # print("RawDataObj error msg: " + str(rawDataObj.error_message))

    if(rawDataObj.is_redirected):  # Checks to see if url has redirected
        used_url = rawDataObj.final_url
        outputLinks.append(rawDataObj.final_url.encode('utf-8'))
    else:
        used_url = rawDataObj.url

    if (rawDataObj.http_code > 399):  # Contains error code
        return outputLinks

    soup = bs.BeautifulSoup(rawDataObj.content.decode('utf-8'), 'lxml')

    for tagObj in soup.find_all('a'):
        if (tagObj.attrs.has_key('href')):
            # print(tagObj['href'].encode('utf-8'))
            outputLinks.append(urljoin(used_url.decode('utf-8'), tagObj['href']).encode('utf-8'))

    return outputLinks


def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    # print("is_valid in URL: " + url)
    global url_query_count, query_cap
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
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf" \
            + "|gctx|npz|npy|bgz|tbi|pbtxt|py|model|hdf5|bed|seq|bw|bam|bigwig|wig|bai|ova)$", parsed.path.lower()) \
            and not re.match(".*calendar.*", parsed.path.lower()) \
            and not re.match(".*/page/[0-9]+", parsed.path.lower()) \
            and not re.match(".*/r[0-9]*a?.html", parsed.path.lower()) \
            and not re.match("/[0-9]+", parsed.path.lower()):
#            and not re.match(".*/.*data/.*", parsed.path.lower()):

            if not url_query_count.has_key(parsed.path.lower()):
                url_query_count[parsed.path.lower()] = 0
            elif url_query_count[parsed.path.lower()] > query_cap:
                return False
            elif len(parsed.query) != 0:
                url_query_count[parsed.path.lower()] += 1

            url_path_last = filter(None, parsed.path.lower().split("/"))
            for i in range(len(url_path_last)):
                if( i == 0 ):
                    continue
                elif( url_path_last[i] == url_path_last[1-1]):
                    return False

            return True
        else:
            return False

    except TypeError:
        print ("TypeError for ", parsed)
        return False
