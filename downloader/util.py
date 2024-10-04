# Copyright (c) Microsoft Corporation.
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing.pool import ThreadPool
import urllib
import io

# def download_url(url, timeout=10):
#     """
#     Download a URL and save its content to a file.
#     :param url: The URL to download.
#     :param timeout: The timeout for the request (in seconds).
#     :return: The URL and the filename.
#     """
#     try:
#         response = requests.get(url['url'], timeout=timeout)
#         response.raise_for_status()  # Ensure we handle HTTP errors
#         # filename = url.split("/")[-1]  # Get the last part of the URL as the filename
        
#         # Save the file
#         # with open(filename, 'wb') as f:
#         #     f.write(response.content)
#         # print(f"Downloaded {filename} from {url}")
#         # return url, filename
#         return response.content, ""
#     except requests.RequestException as e:
#         print(f"Failed to download {url}: {e}")
#         # return url, None
#         return None, str(e)


def download_url(row, timeout=10):
    url = row['url']
    data_stream = None
    user_agent_string = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0"
    try:
        request = urllib.request.Request(url, data=None, headers={"User-Agent": user_agent_string})
        with urllib.request.urlopen(request, timeout=timeout) as r:
            # data_stream = io.BytesIO(r.read())
            data_stream = r.read()
        return data_stream, ""
    except Exception as err:  # pylint: disable=broad-except
        # if data_stream is not None:
        #     data_stream.close()
        return None, str(err)


def download_urls_in_parallel(urls, process_func, max_workers=5):
    """
    Download multiple URLs concurrently using ThreadPoolExecutor.
    :param data: [{"url": xxx, "key_0": xxx, ...}]
    :param max_workers: The number of threads to use for downloading.
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(download_url, url): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                # url, filename = future.result()
                content, error_message = future.result()
                if content:
                    # data = content_process(content)
                    data = {"content": process_func(content, url), 
                            "error_message": ""}
                    # print(f"Successfully downloaded {filename}")
                else:
                    data = {"content": None, "error_message": error_message}
                    # print(f"Download failed for {url}")
            except Exception as e:
                data = {"content": None, "error_message": str(e)}
                # print(f"Error downloading {url}: {e}")
            data.update(url) # append original info
            results.append(data)
    
    # with ThreadPool(max_workers) as thread_pool:
    #     for key, img_stream, error_message in thread_pool.imap_unordered(
    #         lambda x: download_url(
    #             x,
    #             timeout=self.timeout,
    #             retries=self.retries,
    #             user_agent_token=self.user_agent_token,
    #             disallowed_header_directives=self.disallowed_header_directives,
    #         ),
    #         loader,
    #     ):
    
    return results

# # Example usage
# urls = [
#     'https://example.com/file1.jpg',
#     'https://example.com/file2.mp3',
#     'https://example.com/file3.mp4',
#     'https://example.com/file4.pdf'
# ]

# # Download all URLs in parallel
# download_urls_in_parallel(urls, max_workers=5)



if __name__ == "__main__":
    
    data, err = download_url({
        'url': 'http://media.blubrry.com/thegospelfriends/www.thegospelfriends.com/mediafiles/audio/TGF-Ep3-20140615.mp3'
    })
    with open('test_download_url.mp3', 'wb') as file:
        file.write(data)
    print(err)