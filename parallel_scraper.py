from requests_html import HTMLSession
from urllib.parse import urlparse, urljoin, urlunparse
from bs4 import BeautifulSoup
import html2text
import multiprocessing
import os
import time
import sys
import multiprocessing as mp
import tldextract

class Crawler:
    # initialize the set of links (unique links)
    internal_urls = set()
    file_urls = set()
    external_urls = set()
    not_valid_urls = set()
    # corpus= []
    debug = True  # added by ken to control the print() calls
    oErrors = []  # empty list we'll store per domain errors in
    stop_words = set({"n", "callout", "obj", "f", "endobj", "nav"})
    US_URL_EXTENSIONS = ["store", "co" "io", "shop" "blog" "app", "com", "org", "edu", "ai", "gov", "us", "net"]
    corpus = []
    total_urls_visited = 0
    root_url = ""
    domain = ""
    total_url_retries = 0

    proxy_host = "proxy.crawlera.com"
    proxy_port_http = "8010"
    proxy_port_https = "8013"
    proxy_auth = "aabd351a58584477a1ea3492fd25bd3b:"
    proxies = {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port_https),
               "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port_http)}

    def __init__(self, root_url="http://www.oxy.com"):
        self.root_url = root_url
        self.domain = self.get_domain(root_url)

    def get_domain(self, url):
        url_extract = tldextract.extract(url)
        domain = url_extract.domain
        suffix = url_extract.suffix
        '''
        if not any(e in url_extract.suffix for e in self.US_URL_EXTENSIONS):
            domain = url_extract.suffix
        return domain
        '''
        # domain = urlparse(url).netloc

        return domain + "." + suffix

    def save_striped_content(self):
        # sStrippedDomain = urlparse(sFileName).netloc
        # create a directory and put files in it
        if os.path.isdir("./striped_output") == False:
            os.mkdir("./striped_output")
            print("Created striped text folder.")

        # write the striped text to a file and overwrite it
        try:
            file = open("./striped_output/" + self.domain + ".txt", "a", encoding='utf-8')
            content = '\n'.join(self.corpus)
            file.write(content + '\n')
            file.close()
        except Exception as e:
            if self.debug:
                print("There was an exception writing to file " + "./striped_output/ " + self.domain)
                print(e)

    def save_error(self):
        # sStrippedDomain = urlparse(sFileName).netloc
        # create a directory and put files in it
        if os.path.isdir("./error_output") == False:
            os.mkdir("./error_output")
            print("Created error folder.")

        # write the HTML to a file and overwrite it
        try:
            file = open("./error_output/" + self.domain + ".txt", "a", encoding='utf-8')
            content = '\n'.join(self.oErrors)
            file.write(content + '\n')
            file.close()
        except Exception as e:
            if self.debug:
                print("There was an exception writing to file " + "./error_output/ " + self.domain)
                print(e)

    def resolve_response_text(self, response):
        """
        Check response text and make sure we have data to process, OR see if there is a redirect META tag
        returns:
        GOOD_TO_GO - if page appears to have valid data, or
        PAGE_IS_INVALID - if page does NOT appear to have valid data, or
        RETRY_PAGE_GET - if results indicate a transient error (i.e. connection reset), or
        <url> - url string that represents a redirect and needs to be retrieved
        """
        sFirst1000Bytes = ""  # a string to hold the 1st thousand bytes of HTML returned within which we will search for <head
        sRawHtml = ""  # string to save the RAW HTML
        metaContents = ""  # string to extract meta element contents

        sRawHtml = response.html.html
        # print(sRawHtml)
        sFirst1000Bytes = sRawHtml[0:1000]
        sFirst1000Bytes = sFirst1000Bytes.lower()
        if ("<html" not in sFirst1000Bytes) and ("<!doctype html" not in sFirst1000Bytes):
            if self.debug:
                print("No html tag found, looking for redirect via meta tag...")
            if "<meta" not in sFirst1000Bytes:
                if self.debug:
                    print("No meta tag found, looking for retry type responses...")

                # let's consider an empty response possibly caused by transient error and force a retry
                if not (sFirst1000Bytes and not sFirst1000Bytes.isspace()):
                    if self.debug:
                        print("response is empty, retrying: " + response.url)
                    return "RETRY_PAGE_GET"

                # now look for response text that might indicate a transient issue that is worth a retry
                if "connection reset" in sFirst1000Bytes:
                    if self.debug:
                        print("got connection reset for url, retrying: " + response.url)
                    return "RETRY_PAGE_GET"

                if self.debug:
                    print("No retry type responses found, throwing response away!")
                return "PAGE_IS_INVALID"
            else:
                # attempt to find url
                # TODO - NEED BETTER WAY TO DO CASE-INSENSITIVE TAG SEARCH
                content = response.html.xpath('//meta[@http-equiv="refresh"]/@content')
                '''
                if self.debug:
                    print(content)
                '''
                if content:
                    metaContents = content[0].lower()
                    if "url=" in metaContents:
                        # extract url string
                        url = metaContents.split("url=", 1)[1]
                        if self.debug:
                            print("extracted url from meta redirect: " + url)

                        # handle relative redirect paths
                        url_comps = list(urlparse(url))
                        if url_comps[0] == "" or not url_comps[0]:
                            url = urljoin(response.url, url)
                            if self.debug:
                                print("joined relative redirect as: " + url)
                        return url

                if self.debug:
                    print("meta tag does not express redirect.  Throwing response away!")
                return "PAGE_IS_INVALID"
        else:
            # see if we ALSO find a redirect META element (likely inside HEAD element)
            # TODO - NEED BETTER WAY TO DO CASE-INSENSITIVE TAG SEARCH
            content = response.html.xpath('//meta[@http-equiv="refresh"]/@content')
            # print(content)
            if not content:
                # <META HTTP-EQUIV="Refresh" CONTENT="0;URL=http://parkeddomain.earthlink.biz/">
                content = response.html.xpath('//META[@HTTP-EQUIV="Refresh"]/@CONTENT')
                # print(content)

            if content:
                metaContents = content[0].lower()
                if "url=" in metaContents:
                    # extract url string
                    url = metaContents.split("url=", 1)[1]
                    if self.debug:
                        print("extracted url from meta redirect: " + url)

                    # handle relative redirect paths
                    url_comps = list(urlparse(url))
                    if url_comps[0] == "" or not url_comps[0]:
                        url = urljoin(response.url, url)
                        if self.debug:
                            print("joined relative redirect as: " + url)
                    return url

            # assume lack of meta re-direct means we can continue
            return "GOOD_TO_GO"

    def get_all_website_links(self, url):
        """
        Returns all URLs that is found on `url` in which it belongs to the same website
        """
        # all URLs of `url`
        i_urls = set()
        e_urls = set()
        nv_urls = []
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        domain_name = self.get_domain(url)  # domain name of the URL without the protocol
        # print("domain name: " + domain_name)
        session = HTMLSession()  # initialize an HTTP session
        sRawHtml = ""  # string to save the RAW HTML

        # make HTTP request & retrieve response
        if self.debug:
            print("attempting to GET from url: " + url)

        try:
            response = session.get(url)
            if self.debug:
                print("Retrieved response from: " + url)

        except Exception as e:
            if self.debug:
                print("there was an error")
                print(e)
                print("attempting through proxy")
            try:
                response = session.get(url, proxies=self.proxies, verify=False)
                if self.debug:
                    print("Retrieved response (through proxy) from: " + url)
            except Exception as e:
                session.close()
                self.oErrors.append(str(e))
                if self.debug:
                    print("Added error to domain list to save...")
                return i_urls, e_urls, nv_urls

        '''
        # execute Javascript
        try:
            response.html.render()
            if self.debug: print("----------\nSITE HTML:\n" + response.html)
        except:
            print("there was an error rendering to execute javascript")
            pass
        '''
        session.close()

        try:
            # check for <head element
            responseResult = self.resolve_response_text(response)

            if responseResult == "GOOD_TO_GO":
                sRawHtml = response.html.html
                # print(sRawHtml)
                soup = BeautifulSoup(sRawHtml, "html.parser")
                # if self.debug: print("Saving raw HTML to file...")
                # self.save_html(response.html.html, url, sDomain)
                self.corpus = self.corpus + list(soup.stripped_strings)

                for a_tag in soup.findAll(["a", "area"]):
                    href = a_tag.attrs.get("href")
                    if href == "" or href is None:
                        # href empty tag
                        continue
                    # join the URL if it's relative (not absolute link)
                    # print("url = " + url + ", href = " + href)
                    href = urljoin(url, href)
                    # print("joined = " + href)
                    parsed_href = urlparse(href)
                    # print(parsed_href)
                    parsed_comps = list(parsed_href)
                    # remove URL GET parameters, URL fragments, etc.
                    # href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
                    # TODO - REMOVE EXTRANEOUS FORWARD SLASHES
                    parsed_comps[3] = ''
                    parsed_comps[4] = ''
                    parsed_comps[5] = ''
                    href = urlunparse(parsed_comps)
                    # print("HREF = " + href)
                    parsed = urlparse(href)
                    # print(href)
                    if not bool(parsed.netloc) and bool(parsed.scheme):
                        # print("not valid url: " + href)
                        nv_urls.append(href)
                        # not a valid URL
                        continue
                    if domain_name not in href:
                        # print("external url: " + href)
                        # external link
                        e_urls.add(href)
                        continue
                    # print("internal url: " + href)
                    # print(f"{GREEN}[*] Internal link: {href}{RESET}")
                    # TODO - DON'T ADD IF URL IS SAME AS ROOT WITH ONLY SCHEME BEING DIFFERENT
                    i_urls.add(href)
            else:
                if responseResult == "RETRY_PAGE_GET":
                    if self.total_url_retries < 5:
                        self.total_url_retries += 1
                        time.sleep(5)
                        i_urls, e_urls, nv_urls = self.get_all_website_links(url)
                    else:
                        if self.debug:
                            print("too many retries for url: " + url)

                if responseResult != "PAGE_IS_INVALID":
                    # at this point we assume response is new URL to try
                    # TODO - ADD CHECK TO AVOID REDIRECTION TO SAME PAGE
                    i_urls, e_urls, nv_urls = self.get_all_website_links(responseResult)

            return i_urls, e_urls, nv_urls
        except Exception as e:
            if self.debug:
                print("Unhandled exception:")
                print(e)
            self.oErrors.append(str(e))
            return i_urls, e_urls, nv_urls

    def crawl(self, max_depth=20, max_url=100):
        """
        Crawls a web page and extracts all links.
        You'll find all links in `external_urls` and `internal_urls` global set variables.
        params:
            max_urls (int): number of max urls to crawl, default is 30.
        """
        self.internal_urls.add(self.root_url)
        url_queue = [self.root_url]
        depth_queue = [0]
        current_depth = 0
        url = self.root_url
        while len(url_queue) > 0 and (max_depth == None or current_depth < max_depth) and (
                max_depth == None or self.total_urls_visited < max_url):
            self.total_urls_visited += 1
            url = url_queue.pop()
            current_depth = depth_queue.pop()

            # print(self.total_urls_visited)
            i_urls, e_urls, nv_urls = self.get_all_website_links(url)
            self.total_url_retries = 0

            # sorted(i_urls)

            for u in i_urls:
                u = u.lower()
                if u not in self.internal_urls:
                    if (".pdf" not in u) and (".jpg" not in u) and (".png" not in u) and (".wav" not in u) and (
                            ".mp4" not in u) and (".wmv" not in u) and (".zip" not in u) and (".tar" not in u) and (
                            ".tgz" not in u) and (".mp3" not in u) and ("mailto" not in u) and ("jpeg" not in u):
                        # if self.debug: print("Internal: ", u)
                        self.internal_urls.add(u)
                        url_queue.insert(0, u)
                        depth_queue.insert(0, current_depth + 1)
                    else:
                        self.file_urls.add(u)

            for u in e_urls:
                if u not in self.external_urls:
                    self.external_urls.add(u)
                    # if self.debug: print("External: ", u)

            for u in nv_urls:
                if u not in self.not_valid_urls:
                    self.not_valid_urls.add(u)
                    # if self.debug: print("Not valid: ", u)

        self.corpus = set(self.corpus)
        # self.oErrors = set(self.oErrors)


def runprogram(rootUrl):
    pid = os.getpid()
    pid = str(pid)
    crawler = Crawler(rootUrl)
    print("Cralwer for " + crawler.domain + " starting. PID: ", pid, "\n")
    crawler.crawl(5, 50)
    print("Cralwer for " + crawler.domain + " ended. PID: ", pid, "\n")
    crawler.save_striped_content()
    # write errors out to a file in ./html_output/<domain>_errors.txt
    if len(crawler.oErrors) > 0:
        if (crawler.debug): print("Writing errors to file...")
        crawler.save_error()
        return 0
    else:
        return 1


if __name__ == "__main__":
    file_companies = open('./companies.txt', 'r')
    Lines = [l.strip() for l in file_companies.readlines()]
    time1 = time.perf_counter()
    try:
        # mp.freeze_support()
        p = mp.Pool(processes=32)  # default is a number of processes equal to the number of CPU cores
        for i, _ in enumerate(p.imap_unordered(runprogram, Lines), 1):
            sys.stderr.write('\n\r {0:%} total crawling jobs are done\n'.format(i / len(Lines)))

    except Exception as e:
        print(e)
    p.terminate()
    p.join()
    time2 = time.perf_counter()
    runtime = time2 - time1
    print("Started at " + str(time1) + " and finished at " + str(time2) + ". Total runtime: " + str(
        runtime) + " seconds.")

