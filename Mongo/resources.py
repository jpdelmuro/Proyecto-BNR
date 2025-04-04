import falcon
from bson.objectid import ObjectId
from falcon import Request, Response

# ----------- Esquemas de validación -------------
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
    "course_id": str,
    "title": str,
    "description": str,
    "teacher_id": str,
    "lessons": str,
    "created_at": str
}

lesson_types = {
    "lesson_id": str,
    "course_id": str,
    "title": str,
    "content": str,
    "duration": int,
    "resources": str
}

teacher_types = {
    "teacher_id": str,
    "name": str,
    "email": str,
    "password": str,
    "courses_list": str,
    "course_rating": float,
    "created_at": str
}

# ------------- Función de validación ----------------
def validate_data(data, schema_types):
    for prop in schema_types:
        if prop not in data:
            raise falcon.HTTPBadRequest(f"Invalid data: '{prop}' is required.")
        expected_type = schema_types[prop]
        try:
            if expected_type == list and not isinstance(data[prop], list):
                raise ValueError()
            elif expected_type != list:
                data[prop] = expected_type(data[prop])
        except (ValueError, TypeError):
            raise falcon.HTTPBadRequest(f"Invalid data: '{prop}' must be {expected_type.__name__}.")
    return data

# ----------------- Recursos -----------------------

class student_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req: Request, resp: Response, student_id):
        student = await self.db.users.find_one({'_id': ObjectId(student_id)})
        if student:
            student['_id'] = str(student['_id'])
            resp.media = student
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req: Request, resp: Response, student_id):
        data = await req.media
        validate_data(data, student_types)
        result = await self.db.users.update_one(
            {'_id': ObjectId(student_id)},
            {'$set': data}
        )
        if result.modified_count:
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_delete(self, req: Request, resp: Response, student_id):
        result = await self.db.users.delete_one({'_id': ObjectId(student_id)})
        if result.deleted_count:
            resp.status = falcon.HTTP_204
        else:
            resp.status = falcon.HTTP_404

class course_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, course_id):
        course = await self.db.courses.find_one({'_id': ObjectId(course_id)})
        if course:
            course['_id'] = str(course['_id'])
            resp.media = course
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, course_id):
        data = await req.media
        validate_data(data, course_types)
        result = await self.db.courses.update_one(
            {'_id': ObjectId(course_id)},
            {'$set': data}
        )
        if result.modified_count:
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_delete(self, req, resp, course_id):
        result = await self.db.courses.delete_one({'_id': ObjectId(course_id)})
        if result.deleted_count:
            resp.status = falcon.HTTP_204
        else:
            resp.status = falcon.HTTP_404

class lesson_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, lesson_id):
        lesson = await self.db.lessons.find_one({'_id': ObjectId(lesson_id)})
        if lesson:
            lesson['_id'] = str(lesson['_id'])
            resp.media = lesson
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, lesson_id):
        data = await req.media
        validate_data(data, lesson_types)
        result = await self.db.lessons.update_one(
            {'_id': ObjectId(lesson_id)},
            {'$set': data}
        )
        if result.modified_count:
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_delete(self, req, resp, lesson_id):
        result = await self.db.lessons.delete_one({'_id': ObjectId(lesson_id)})
        if result.deleted_count:
            resp.status = falcon.HTTP_204
        else:
            resp.status = falcon.HTTP_404

class instructor_resource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, instructor_id):
        instructor = await self.db.instructors.find_one({'_id': ObjectId(instructor_id)})
        if instructor:
            instructor['_id'] = str(instructor['_id'])
            resp.media = instructor
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, instructor_id):
        data = await req.media
        validate_data(data, teacher_types)
        result = await self.db.instructors.update_one(
            {'_id': ObjectId(instructor_id)},
            {'$set': data}
        )
        if result.modified_count:
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_delete(self, req, resp, instructor_id):
        result = await self.db.instructors.delete_one({'_id': ObjectId(instructor_id)})
        if result.deleted_count:
            resp.status = falcon.HTTP_204
        else:
            resp.status = falcon.HTTP_404


