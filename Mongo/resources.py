import falcon
from bson.objectid import ObjectId

class student_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, student_id):
        """Handles GET requests to retrieve a student"""
        user = self.db.users.find_one({'_id': ObjectId(student_id)})
        if student:
            student['_id'] = str(student['_id'])
            resp.media = student
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, student_id):
        """Handles PUT requests to update a single student"""
        pass

    async def on_delete(self, req, resp, student_id):
        """Handles DELETE requests to delete a single student"""
        pass

class course_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, course_id):
        """Handles GET requests to retrieve a course"""
        course = self.db.courses.find_one({'_id': ObjectId(course_id)})
        if course:
            course['_id'] = str(course['_id'])
            resp.media = course
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, course_id):
        """Handles PUT requests to update a single course"""
        pass

    async def on_delete(self, req, resp, course_id):
        """Handles DELETE requests to delete a single course"""
        pass

class lesson_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, lesson_id):
        """Handles GET requests to retrieve a lesson"""
        lesson = self.db.lessons.find_one({'_id': ObjectId(lesson_id)})
        if lesson:
            lesson['_id'] = str(lesson['_id'])
            resp.media = lesson
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, lesson_id):
        """Handles PUT requests to update a single lesson"""
        pass

    async def on_delete(self, req, resp, lesson_id):
        """Handles DELETE requests to delete a single lesson"""
        pass
    

class instructor_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, instructor_id):
        """Handles GET requests to retrieve a instructor"""
        instructor = self.db.instructors.find_one({'_id': ObjectId(instructor_id)})
        if instructor:
            instructor['_id'] = str(instructor['_id'])
            resp.media = instructor
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, instructor_id):
        """Handles PUT requests to update a single instructor"""
        pass

    async def on_delete(self, req, resp, instructor_id):
        """Handles DELETE requests to delete a single instructor"""
        pass


student_types = {
    "student_id": str,
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

