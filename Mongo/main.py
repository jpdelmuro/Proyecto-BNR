import falcon.asgi
from pymongo import MongoClient
import logging

from resources import student_resource, course_resource, lesson_resource, instructor_resource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoggingMiddleware:
    async def process_request(self, req, resp):
        logger.info(f"Request: {req.method} {req.uri}")

    async def process_response(self, req, resp, resource, req_succeeded):
        logger.info(f"Response: {resp.status} for {req.method} {req.uri}")

# Initialize MongoDB client and database
client = MongoClient('mongodb://localhost:27017/')
db = client.PROJECT

# Create the Falcon application
app = falcon.asgi.App(middleware=[LoggingMiddleware()])

# Instantiate the resources
user_resource = student_resource(db)
course_resource = course_resource(db)
lesson_resource = lesson_resource(db)
instructor_resource = instructor_resource(db)

# Add routes to serve the resources
#app.add_route('/books', books_resource)
#app.add_route('/books/{book_id}', book_resource)
