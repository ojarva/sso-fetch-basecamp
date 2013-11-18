import json
import httplib2
import base64
from config import Config
import redis
from instrumentation import *
import xmltodict

class BasecampClassic:
    def __init__(self):
        self.h = httplib2.Http(disable_ssl_certificate_validation=True)
        self.config = Config()
        self.auth = base64.encodestring( self.config.get("basecamp-username") + ':' + self.config.get("basecamp-password"))
        self.headers = {"Authorization": "Basic "+ self.auth}
        self.base_url = self.config.get("basecamp-classic-base-url")
        self.redis = redis.Redis(host=self.config.get("redis-hostname"), port=self.config.get("redis-port"), db=self.config.get("redis-db"))
        self.post_queue = []
        self._people = None
        self.last_timestamp = self.load_last_timestamp()
        self.last_timestamp_save = self.last_timestamp

    def get_data(self, url, key, expire=1700):
        r_key = "basecamp-classic-tmp-%s" % key
        if self.redis.exists(r_key):
            return json.loads(self.redis.get(r_key))
        resp, content = self.h.request(self.base_url+url, "GET", headers=self.headers)
        d = xmltodict.parse(content)
        data = json.loads(json.dumps(d))
        self.redis.setex(r_key, json.dumps(data), expire)
        return data
        
    def get_people(self):
        if self._people is None:
            self._people = self.get_data("people.xml", "get_people", 3600)
        return self._people
       

    def get_projects(self):
        return self.get_data("projects.xml", "get_projects", 3600)

    def get_messages(self, project_id):
        return self.get_data("projects/%s/posts.xml" % project_id, "get_messages2_%s" % project_id, 250)

    def get_comments(self, message_id, expected_count = 0):
        return self.get_data("posts/%s/comments.xml" % message_id, "get_comments_msg_%s_%s" % (message_id, expected_count), 86400 )

    def get_email(self, user_id):
        for person in self.get_people()["people"]["person"]:
            if person.get("id", {}).get("#text") == user_id:
                return person["email-address"]
        return None

    def get_files(self, project_id, offset=0):
        return self.get_data("projects/%s/attachments.xml?offset=%s" % (project_id, offset), "get_attachments_%s_%s" % (project_id, offset), 1800)

    def process(self):
        projects = self.get_projects()
        for project in projects.get("projects", {}).get("project", []):
            project_id = project["id"]["#text"]
            latest_messages = self.get_messages(project_id)
            for message in latest_messages.get("posts", {}).get("post", []):
                if not isinstance(message, dict):
                    continue
                message_id = message["id"]["#text"]
                author_id = message["author-id"]["#text"]
                author = self.get_email(author_id)
                message_timestamp = message["posted-on"]["#text"]
                if author is not None:
                    d = {"system": "basecamp_classic", "username": author, "data": "message-%s" % message_id, "timestamp": message_timestamp}
                    self.post(d)

                cc = message["comments-count"]["#text"]
                if cc != self.redis.get("basecamp-classic-tmp-msg-com-%s" % message_id) and int(cc) > 0:
                    comments = self.get_comments(message_id, cc)
                    for comment in comments.get("comments", {}).get("comment"):
                        if not isinstance(comment, dict):
                            continue
                        comment_author_id = comment["author-id"]["#text"]
                        comment_timestamp = comment["created-at"]["#text"]
                        comment_id = comment["id"]["#text"]
                        comment_author = self.get_email(comment_author_id)
                        if comment_author is None:
                            continue
                        d = {"system": "basecamp_classic", "username": comment_author, "data": "comment-%s" % comment_id, "timestamp": comment_timestamp}
                        self.post(d)
                    self.redis.setex("basecamp-classic-tmp-msg-com-%s" % message_id, cc, 86400 * 30)
            self.post_finished()

            offset = 0
            while True:
                files = self.get_files(project_id, offset)
                try:
                    filelist = files.get("attachments", {}).get("attachment", [])
                except AttributeError:
                    break
                for a_file in filelist:
                    if not isinstance(a_file, dict):
                        continue
                    file_owner_id = a_file.get("person-id", {}).get("#text")
                    file_owner = self.get_email(file_owner_id)
                    file_timestamp = a_file.get("created-on", {}).get("#text")
                    file_id = a_file.get("id", {}).get("#text")
                    if file_timestamp is None or file_owner is None:
                        continue
                    d = {"system": "basecamp_classic", "username": file_owner, "data": "file-%s" % file_id, "timestamp": file_timestamp}
                    self.post(d)
                self.post_finished()
                if len(files) == 100:
                    offset += 100
                else:
                    break
        self.save_last_timestamp(self.last_timestamp_save)

    def post_finished(self):
        self.post()

    @timing("basecamp.classic.update.post")
    def post(self, data = None):
        if data:
            if data["timestamp"] > self.last_timestamp_save:
                self.last_timestamp_save = data["timestamp"]
            if data["timestamp"] > self.last_timestamp:
                self.post_queue.append(data)

        if len(self.post_queue) > 100 or (data is None and len(self.post_queue) > 0):
            statsd.incr("basecamp.classic.update.post.request")
            (resp, cont) = self.h.request(self.config.get("server-url"), "POST", body=json.dumps(self.post_queue))
            if cont == "OK":
                self.post_queue = []
            else:
                return False


    def save_last_timestamp(self, timestamp):
        r_key = "basecamp-classic-last_timestamp"
        self.redis.set(r_key, timestamp)

    def load_last_timestamp(self):
        r_key = "basecamp-classic-last_timestamp"
        if self.redis.exists(r_key):
            return self.redis.get(r_key)
        else:
            return "2000-01-01T00:00:00+02:00"

def main():
    bcc = BasecampClassic()
    bcc.process()

if __name__ == '__main__':
    main()
