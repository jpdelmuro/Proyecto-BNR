import falcon
from bson.objectid import ObjectId

class student_resource:

class course_resource:

class lesson_resource:

class instructor_resource:


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
