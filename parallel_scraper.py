from requests_html import HTMLSession
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import html2text
import multiprocessing
import os
import time
import sys
import multiprocessing as mp


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
        '''
        url_extract = tldextract.extract(url)
        domain = url_extract.domain
        if not any(e in url_extract.suffix for e in self.US_URL_EXTENSIONS):
            domain = url_extract.suffix
        return domain
        '''
        domain = urlparse(url).netloc
        return domain

    def save_striped_content(self):
        # sStrippedDomain = urlparse(sFileName).netloc
        # create a directory and put files in it
        if os.path.isdir("./striped_output") == False:
            os.mkdir("./striped_output")
            print("Created striped text folder.")

        # write the striped text to a file and overwrite it
        try:
            file = open("./striped_output/" + self.domain, "a", encoding='utf-8')
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
            file = open("./error_output/" + self.domain, "a", encoding='utf-8')
            content = '\n'.join(self.oErrors)
            file.write(content + '\n')
            file.close()
        except Exception as e:
            if self.debug:
                print("There was an exception writing to file " + "./error_output/ " + self.domain)
                print(e)

    def get_all_website_links(self, url):
        """
        Returns all URLs that is found on `url` in which it belongs to the same website
        """
        # all URLs of `url`
        i_urls = set()
        e_urls = set()
        nv_urls = []
        sFirst1000Bytes = ""  # a string to hold the 1st thousand bytes of HTML returned within which we will search for <head
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        domain_name = self.get_domain(url)  # domain name of the URL without the protocol
        session = HTMLSession()  # initialize an HTTP session
        sRawHtml = ""  # string to save the RAW HTML

        # make HTTP request & retrieve response
        try:
            response = session.get(url)
            if self.debug: print("Retrieved response from: " + url)

        except Exception as e:
            if self.debug:
                print("there was an error")
                print(e)
                print("attempting through proxy")
            try:
                response = session.get(url, proxies=self.proxies, verify=False)
                if self.debug: print("Retrieved response (through proxy) from: " + url)
            except Exception as e:
                session.close()
                self.oErrors.append(str(e))
                if self.debug: print("Added error to domain list to save...")
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
            sRawHtml = response.html.html
            sFirst1000Bytes = sRawHtml[0:1000]
            sFirst1000Bytes = sFirst1000Bytes.lower()
            if "<html" not in sFirst1000Bytes:
                if self.debug:
                    print("No html tag found.  Throwing response away...")

            else:
                soup = BeautifulSoup(sRawHtml, "html.parser")
                # if self.debug: print("Saving raw HTML to file...")
                # self.save_html(response.html.html, url, sDomain)
                self.corpus = self.corpus + list(soup.stripped_strings)

                for a_tag in soup.findAll("a"):
                    href = a_tag.attrs.get("href")
                    if href == "" or href is None:
                        # href empty tag
                        continue
                    # join the URL if it's relative (not absolute link)
                    href = urljoin(url, href)
                    parsed_href = urlparse(href)
                    # remove URL GET parameters, URL fragments, etc.
                    href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
                    parsed = urlparse(href)
                    if not bool(parsed.netloc) and bool(parsed.scheme):
                        nv_urls.append(href)
                        # not a valid URL
                        continue
                    if domain_name not in href:
                        # external link
                        e_urls.add(href)
                        continue
                    # print(f"{GREEN}[*] Internal link: {href}{RESET}")
                    i_urls.add(href)
            return i_urls, e_urls, nv_urls
        except Exception as e:
            if self.debug == True:
                print("Unhandled exception:")
                print(e)
            self.oErrors.append(str(e))
            return i_urls, e_urls, nv_urls

    def crawl(self, max_depth=20, max_url=20):
        """
        Crawls a web page and extracts all links.
        You'll find all links in `external_urls` and `internal_urls` global set variables.
        params:
            max_urls (int): number of max urls to crawl, default is 30.
        """
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

            for u in i_urls:
                u = u.lower()
                if u not in self.internal_urls:
                    if (".pdf" not in u) and (".jpg" not in u) and (".png" not in u) and (".wav" not in u) and (
                            ".mp4" not in u) and (".wmv" not in u) and (".zip" not in u) and (".tar" not in u) and (
                            ".tgz" not in u) and (".mp3" not in u) and ("mailto" not in u):
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

