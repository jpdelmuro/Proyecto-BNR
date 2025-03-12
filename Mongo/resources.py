import falcon
from bson.objectid import ObjectId

class student_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, book_id):
        """Handles GET requests to retrieve a single user"""
        user = self.db.users.find_one({'_id': ObjectId(user_id)})
        if user:
            user['_id'] = str(user['_id'])
            resp.media = user
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, user_id):
        """Handles PUT requests to update a single user"""
        pass

    async def on_delete(self, req, resp, user_id):
        """Handles DELETE requests to delete a single user"""
        pass

class course_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, book_id):
        """Handles GET requests to retrieve a single user"""
        course = self.db.courses.find_one({'_id': ObjectId(course_id)})
        if course:
            course['_id'] = str(course['_id'])
            resp.media = course
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, course_id):
        """Handles PUT requests to update a single user"""
        pass

    async def on_delete(self, req, resp, course_id):
        """Handles DELETE requests to delete a single user"""
        pass

class lesson_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, book_id):
        """Handles GET requests to retrieve a single user"""
        lesson = self.db.lessons.find_one({'_id': ObjectId(lesson_id)})
        if lesson:
            lesson['_id'] = str(lesson['_id'])
            resp.media = lesson
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, lesson_id):
        """Handles PUT requests to update a single user"""
        pass

    async def on_delete(self, req, resp, lesson_id):
        """Handles DELETE requests to delete a single user"""
        pass
    

class instructor_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, book_id):
        """Handles GET requests to retrieve a single user"""
        instructor = self.db.instructors.find_one({'_id': ObjectId(instructor_id)})
        if instructor:
            instructor['_id'] = str(instructor['_id'])
            resp.media = instructor
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, instructor_id):
        """Handles PUT requests to update a single user"""
        pass

    async def on_delete(self, req, resp, instructor_id):
        """Handles DELETE requests to delete a single user"""
        pass


student_types = {
    "user_id": str,
    "name": str,
    "email": str,
    "password": str,
    "courses_enrolled": list,
    "courses_completed": list,
    "created_at": str,
}

course_types = {
  "course_id":str,
  "tite":str,
  "description":str,
  "teacher_id":str,
  "lessons":str,
  "created_at":str
}

lesson_types = {
    "lesson_id":str,
    "course_id":str,
    "title":str,
    "content":str,
    "duration":int,
    "resources":str
}

teacher_types = {
    "teacher_id":str,
    "name":str,
    "email":str
    "password":str
    "courses_list":str
    "course_rating":float
    "created_at":str
}

#Validate data
"""
def validate_data(data):
    for property in book_types:
        if property not in data: 
            raise falcon.HTTPBadRequest(f"Invalid data: {property} is required.")
        if book_types[property] != str:
            try:
                data[property] = book_types[property](data[property])
            except ValueError:
                raise falcon.HTTPBadRequest(f"Invalid data: {property} must be {book_types[property]}.")
    return data
    """

