import json
import httplib2
import base64
from config import Config
import redis
from instrumentation import *

class BasecampNew:
    def __init__(self):
        self.h = httplib2.Http(disable_ssl_certificate_validation=True)
        self.config = Config()
        self.auth = base64.encodestring( self.config.get("basecamp-username") + ':' + self.config.get("basecamp-password"))
        self.headers = {"Authorization": "Basic "+ self.auth}
        self.base_url = "https://basecamp.com/%s/api/v1/" % self.config.get("basecamp-companyid")
        self.redis = redis.Redis(host=self.config.get("redis-hostname"), port=self.config.get("redis-port"), db=self.config.get("redis-db"))
        self.post_queue = []

    def get_data(self, url, key):
        r_key = "basecamp-tmp-%s" % key
        if self.redis.exists(r_key):
            return json.loads(self.redis.get(r_key))
        _, content = self.h.request(self.base_url+url, "GET", headers=self.headers)
        data = json.loads(content)
        self.redis.setex(r_key, json.dumps(data), 1700)
        return data
        
    def get_people(self):
        return self.get_data("people.json", "get_people")

    def get_projects(self):
        return self.get_data("projects.json", "get_projects")

    def get_all_topics(self, page=1):
        return self.get_data("topics.json?page=%s" % page, "get_all_topics_%s" % page)

    def get_events(self, since, page=1):
        return self.get_data("events.json?since=%s&page=%s" % (since, page), "get_events_%s_%s" % (since, page))

    def process(self):
        last_timestamp = self.load_last_timestamp()
        last_timestamp_save = last_timestamp
        people = self.get_people()
        people_map = {}
        for person in people:
            people_map[person["id"]] = person.get("email_address")

        page = 1
        while True:
            events = self.get_events(last_timestamp, page)
            for event in events:
                ts = event.get("created_at")
                username = people_map.get(event.get("creator", {}).get("id"))
                if ts is None or username is None:
                    continue
                self.post({"system": "basecamp_new", "timestamp": ts, "username": username})
                if ts > last_timestamp_save:
                    last_timestamp_save = ts
            self.post_finished()
            self.save_last_timestamp(last_timestamp_save)
            page += 1
            if len(events) < 50:
                break

    def post_finished(self):
        self.post()

    @timing("basecamp.update.post")
    def post(self, data = None):
        if data is not None:
            self.post_queue.append(data)
        if len(self.post_queue) > 100 or (data is None and len(self.post_queue) > 0):
            statsd.incr("basecamp.update.post.request")
            (_, cont) = self.h.request(self.config.get("server-url"), "POST", body=json.dumps(self.post_queue))
            if cont == "OK":
                self.post_queue = []
            else:
                return False

    def save_last_timestamp(self, timestamp):
        r_key = "basecamp-last_timestamp"
        self.redis.set(r_key, timestamp)

    def load_last_timestamp(self):
        r_key = "basecamp-last_timestamp"
        if self.redis.exists(r_key):
            return self.redis.get(r_key)
        else:
            return "2000-01-01T00:00:00+02:00"

def main():
    bcn = BasecampNew()
    bcn.process()

if __name__ == '__main__':
    main()
