from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session

from pprint import pprint


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    """
    return session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Student) \
        .group_by(Student.id) \
        .order_by(desc('avg_grade')) \
        .limit(5) \
        .all()


def select_2(discipline_id):
    """
    Знайти студента із найвищим середнім балом з певного предмета.
    """
    return session.query(Discipline.name, Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()


def select_3():
    """
    Знайти середній бал у групах з певного предмета
    """
    return session.query(Discipline.name, Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .group_by(Group.name, Discipline.name) \
        .all()


def select_4():
    """
    Знайти середній бал на потоці (по всій таблиці оцінок).
    """
    return session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .all()


def select_5():
    """
    Знайти які курси читає певний викладач.
    """
    return session.query(Teacher.fullname, Discipline.name) \
        .select_from(Teacher) \
        .join(Discipline) \
        .group_by(Discipline.name, Teacher.fullname) \
        .order_by(Teacher.fullname) \
        .all()


def select_6(group_id):
    """
    Знайти список студентів у певній групі.
    """
    return session.query(Student.fullname, Group.name) \
        .select_from(Group) \
        .join(Student) \
        .where(Group.id == group_id) \
        .all()


def select_7(group_id, discipline_id):
    """
    Знайти оцінки студентів у окремій групі з певного предмета.
    """
    return session.query(Student.fullname, Group.name, Discipline.name, Grade.grade, Grade.date_of) \
        .select_from(Group) \
        .join(Student) \
        .join(Grade) \
        .join(Discipline) \
        .where(and_(Group.id == group_id, Discipline.id == discipline_id)) \
        .all()


def select_8(teacher_id):
    """
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    """
    return session.query(Teacher.fullname, Discipline.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Teacher) \
        .join(Discipline) \
        .join(Grade) \
        .where(Teacher.id == teacher_id) \
        .group_by(Teacher.fullname, Discipline.name) \
        .order_by('avg_grade') \
        .all()


def select_9(student_id):
    """
    Знайти список курсів, які відвідує певний студент.
    """
    return session.query(Student.fullname, Discipline.name) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .where(Student.id == student_id) \
        .group_by(Student.fullname, Discipline.name) \
        .all()


def select_10(student_id, teacher_id):
    """
    Список курсів, які певному студенту читає певний викладач.
    """
    return session.query(Student.fullname, Teacher.fullname, Discipline.name) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Teacher) \
        .where(and_(Student.id == student_id, Teacher.id == teacher_id)) \
        .group_by(Discipline.name, Student.fullname, Teacher.fullname) \
        .all()


def select_11(teacher_id, student_id):
    """
    Середній бал, який певний викладач ставить певному студентові.
    """
    return session.query(Student.fullname, Teacher.fullname, Discipline.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Teacher) \
        .where(and_(Student.id == student_id, Teacher.id == teacher_id)) \
        .group_by(Discipline.name, Student.fullname, Teacher.fullname) \
        .all()


def select_12(discipline_id, group_id):
    """
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    """
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    return session.query(Discipline.name, Student.fullname, Group.name, Grade.date_of, Grade.grade) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group)\
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()


if __name__ == '__main__':
    pprint(select_1())
    pprint(select_2(1))
    pprint(select_3())
    pprint(select_4())
    pprint(select_5())
    pprint(select_6(2))
    pprint(select_7(1, 1))
    pprint(select_8(1))
    pprint(select_9(6))
    pprint(select_10(1, 3))
    pprint(select_11(3, 5))
    pprint(select_12(1, 2))

