



fakeDataCommand = """
insert into patients_details (id, mrn, first_name, last_name, zip_code, dob, gender, contact_mobile,  contact_home) values (1, '0001', 'john', 'smith', '10012', '01/01/1990', 'male', '621-555-5555', '212-555-5555');
"""

fakeDataCommand2 = """
insert into patients_details values (2222, '0001', 'john', 'smith', '10012', '01/01/1990', 'male', '621-555-5555', '212-555-5555');
"""

fakeDataCommand3 = """
insert into patients_details values 
(3, '0001', 'john', 'smith', '10012', '01/01/1990', 'male', '621-555-5555', '212-555-5555'), 
(4, '0001', 'john', 'smith', '10012', '01/01/1990', 'male', '621-555-5555', '212-555-5555'), 
(5, '0001', 'john', 'smith', '10012', '01/01/1990', 'male', '621-555-5555', '212-555-5555')
;
"""

testSql = """
insert into patients_details_2 (last_name, first_name) values 
('trump', 'donald')
;
"""
db.execute(testSql)

db.execute(fakeDataCommand)
db.execute(fakeDataCommand2)
db.execute(fakeDataCommand3)