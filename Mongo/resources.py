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

