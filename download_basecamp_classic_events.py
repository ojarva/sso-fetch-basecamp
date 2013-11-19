"""
Downloads timestamps and usernames for messages, files and comments from
Basecamp Classic, and posts data to SSO analytics
"""

# pylint: disable=C0301
import json
import httplib2
import base64
from config import Config
import redis
from instrumentation import *
import xmltodict

class BasecampClassic:
    """ Limited API for Basecamp classic """
    def __init__(self):
        self.h = httplib2.Http(disable_ssl_certificate_validation=True)
        self.config = Config()
        auth = base64.encodestring( self.config.get("basecamp-username") + ':' + self.config.get("basecamp-password"))
        self.headers = {"Authorization": "Basic "+ auth}
        self.redis = redis.Redis(host=self.config.get("redis-hostname"),
                                port=self.config.get("redis-port"),
                                db=self.config.get("redis-db"))
        self.post_queue = []
        self._people = None
        self.last_timestamp = self.load_last_timestamp()
        self.last_timestamp_save = self.last_timestamp

    def get_data(self, url, key, expire=1700):
        """ Downloads data from basecamp, and stores results in redis """
        r_key = "basecamp-classic-tmp-%s" % key
        base_url = self.config.get("basecamp-classic-base-url")
        if self.redis.exists(r_key):
            return json.loads(self.redis.get(r_key))
        _, content = self.h.request(base_url+url, "GET", headers=self.headers)
        data_raw = xmltodict.parse(content)
        data = json.loads(json.dumps(data_raw))
        self.redis.setex(r_key, json.dumps(data), expire)
        return data
        
    def get_comments(self, message_id, expected_count = 0):
        """ Returns list of comments for message_id """
        return self.get_data("posts/%s/comments.xml" % message_id, "get_comments_msg_%s_%s" % (message_id, expected_count), 86400 )

    def get_email(self, user_id):
        """ Returns email address for user_id, or None """
        for person in self.get_people()["people"]["person"]:
            if person.get("id", {}).get("#text") == user_id:
                return person["email-address"]
        return None

    def get_files(self, project_id, offset=0):
        """ Returns list of files for project from given offset """
        return self.get_data("projects/%s/attachments.xml?offset=%s" % (project_id, offset), "get_attachments_%s_%s" % (project_id, offset), 1800)

    def get_messages(self, project_id):
        """ Returns list of message for project """
        return self.get_data("projects/%s/posts.xml" % project_id, "get_messages2_%s" % project_id, 250)

    def get_people(self):
        """ Returns (cached) list of people in organization"""
        if self._people is None:
            self._people = self.get_data("people.xml", "get_people", 3600)
        return self._people

    def get_projects(self):
        """ Returns list of projects for organization """
        return self.get_data("projects.xml", "get_projects", 3600)

    def _process_message(self, message):
        """ Processes a single message from Basecamp, including downloading
            all comments """
        if not isinstance(message, dict):
            return
        message_id = message["id"]["#text"]
        author_id = message["author-id"]["#text"]
        author = self.get_email(author_id)
        message_timestamp = message["posted-on"]["#text"]
        if author is not None:
            data = {"system": "basecamp_classic", "username": author, "data": "message-%s" % message_id, "timestamp": message_timestamp}
            self.post(data)

        comment_c = message["comments-count"]["#text"]
        if comment_c != self.redis.get("basecamp-classic-tmp-msg-com-%s" % message_id) and int(comment_c) > 0:
            comments = self.get_comments(message_id, comment_c)
            for comment in comments.get("comments", {}).get("comment"):
                if not isinstance(comment, dict):
                    continue
                comment_author_id = comment["author-id"]["#text"]
                comment_timestamp = comment["created-at"]["#text"]
                comment_id = comment["id"]["#text"]
                comment_author = self.get_email(comment_author_id)
                if comment_author is None:
                    continue
                data = {"system": "basecamp_classic", "username": comment_author, "data": "comment-%s" % comment_id, "timestamp": comment_timestamp}
                self.post(data)
            self.redis.setex("basecamp-classic-tmp-msg-com-%s" % message_id, comment_c, 86400 * 30)

    def _process_file(self, a_file):
        """ Processes a single file item from Basecamp """
        if not isinstance(a_file, dict):
            return
        file_owner_id = a_file.get("person-id", {}).get("#text")
        file_owner = self.get_email(file_owner_id)
        file_timestamp = a_file.get("created-on", {}).get("#text")
        file_id = a_file.get("id", {}).get("#text")
        if file_timestamp is None or file_owner is None:
            return
        data = {"system": "basecamp_classic", "username": file_owner, "data": "file-%s" % file_id, "timestamp": file_timestamp}
        self.post(data)

    def process(self):
        """ Fetches all projects, messages, comments and files from
            Basecamp, and stores usernames and timestamps """
        projects = self.get_projects()
        for project in projects.get("projects", {}).get("project", []):
            project_id = project["id"]["#text"]
            latest_messages = self.get_messages(project_id)
            for message in latest_messages.get("posts", {}).get("post", []):
                self._process_message(message)
            self.post()

            offset = 0
            while True:
                files = self.get_files(project_id, offset)
                try:
                    filelist = files.get("attachments", {}).get("attachment", [])
                except AttributeError:
                    break
                for a_file in filelist:
                    self._process_file(a_file)
                self.post()
                if len(files) == 100:
                    offset += 100
                else:
                    break
        self.save_last_timestamp(self.last_timestamp_save)

    @timing("basecamp.classic.update.post")
    def post(self, data = None):
        """ Adds data to post queue. If data is none or queue is over 100
            entries, everything is posted and queue is emptied. """
        if data:
            if data["timestamp"] > self.last_timestamp_save:
                self.last_timestamp_save = data["timestamp"]
            if data["timestamp"] > self.last_timestamp:
                self.post_queue.append(data)

        if len(self.post_queue) > 100 or (data is None and len(self.post_queue) > 0):
            statsd.incr("basecamp.classic.update.post.request")
            (_, cont) = self.h.request(self.config.get("server-url"), "POST", body=json.dumps(self.post_queue))
            if cont == "OK":
                self.post_queue = []
            else:
                return False


    def save_last_timestamp(self, timestamp):
        """ Saves the last timestamp to redis """
        r_key = "basecamp-classic-last_timestamp"
        self.redis.set(r_key, timestamp)

    def load_last_timestamp(self):
        """ Loads the last timestamp from redis """
        r_key = "basecamp-classic-last_timestamp"
        if self.redis.exists(r_key):
            return self.redis.get(r_key)
        else:
            return "2000-01-01T00:00:00+02:00"

def main():
    """ Processes all new entries """
    bcc = BasecampClassic()
    bcc.process()

if __name__ == '__main__':
    main()
