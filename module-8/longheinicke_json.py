#Jessica Long-Heinicke Module 8

import json

#load json data into a python class list
def load_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

#function to print student list
def print_student_list(student_list):
    for student in student_list:
        print(f"{student['F_Name']} , {student['L_Name']} :ID = {student['Student_ID']} , Email = {student['Email']}")

#append new student data to the list
def append_student (student_list, first_name, last_name, student_id, email):
    new_student = {
        "F_Name": first_name,
        "L_Name": last_name,
        "Student_ID": student_id,
        "Email": email
    }
    student_list.append(new_student)

#save updated data back to the json file
def save_to_json(file_path, student_list):
    with open(file_path, 'w') as file:
        json.dump(student_list, file, indent=4)

#file path to the json file
json_file = "students.json"

#load the student data
students = load_json(json_file)

#print the original list
print("This is the original student list:")
print_student_list(students)
print()

#add new student
append_student(students, "Jessica", "Long-Heinicke", "12345", "jlongheinicke@my365.bellevue.edu")

#print updated list
print("This is the updated student list:")
print_student_list(students)
print()

#save the updated data back ot hte json file
save_to_json("students.json", students)
print("the .json file was updated.")