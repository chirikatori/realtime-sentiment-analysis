import requests
from data_streaming import Config


# This file is only use for scrape data, you should build your own scrape class
# with your API or another ways
class ScrapeData():
    def __init__(self,
                 url: str = Config.url,
                 query_params: dict = Config.querystring,
                 headers: dict = Config.headers):
        self.url = url
        self.query_params = query_params
        self.headers = headers

    def get_comments(self):
        response = requests.get(url=self.url,
                                headers=self.headers,
                                params=self.query_params)
        response = response.json()
        comments = []
        for comment in response["comments"]:
            comments.append(comment["text"])
        return comments
