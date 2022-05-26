import xlwt
import numpy as np

excel = xlwt.Workbook(encoding='utf-8', style_compression=0)
student_sheet = excel.add_sheet('student', cell_overwrite_ok=True)
sc_sheet = excel.add_sheet('sc', cell_overwrite_ok=True)
student_col = ('s_id', 'cl_id', 's_name', 's_sex', 's_age', 's_tel')
sc_col = ('s_id', 'c_id', 'score', 'date')

for i in range(len(student_col)):
    student_sheet.write(0, i, student_col[i])
for i in range(len(sc_col)):
    sc_sheet.write(0, i, sc_col[i])

for i in range(1000):
    s_id = '100403' + '{:03d}'.format(i)
    cl_id = '100403'
    s_name = 'TEST' + str(i)
    s_sex = '男' if np.random.rand() < 0.5 else '女'
    s_age = np.random.randint(18, 24)
    s_tel = '13000000' + '{:03d}'.format(i)
    c_id = '130001'
    score = str(np.random.randint(1, 100))
    date = '2022-01-01'
    student_list = (s_id, cl_id, s_name, s_sex, s_age, s_tel)
    sc_list = (s_id, c_id, score, date)
    for j in range(len(student_list)):
        student_sheet.write(i + 1, j, student_list[j])
    for j in range(len(sc_list)):
        sc_sheet.write(i + 1, j, sc_list[j])

excel.save('./gen_data.xls')
