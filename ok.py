from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()


medication=spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/MEDICATION.txt")
nurse = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/nurse.txt")
on_call = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/on_call.txt",parse_dates=['oncall'])
patient = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/patient.txt")
physician = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/physician.txt")
prescribes = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/prescribes.txt",parse_dates=['date'])
procedures = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/procedures.txt")
room = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/room.txt")
stay = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/stay.txt",parse_dates=['start_time','end_time'])
trained_in = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/trained_in.txt",parse_dates=['certificationdate','certificationexpires'])
undergoes = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/Undergoes.txt",parse_dates=['date'])
affiliated_with = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/affiliated_with.txt")
appointment = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/appointment.txt",parse_dates=['start_dt','end_dt'])
block = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/block.txt")
department = spark.read.format("csv").options(header=True,inferSchema=True).load("D:/Python/python_project/PythonProject_HospitalManagementSystem/Python_project_datasets/department.txt")

physician.show()
department.show()

physician.join(department,physician.employeeid == department["head"],"inner").show()