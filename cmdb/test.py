import xlwt
from io import BytesIO


profile = xlwt.Workbook()
sheet = profile.add_sheet("sheet")
for i in range(10):
    for j in range(10):
        sheet.write(i, j, f'{i},{j}')

i = BytesIO()
profile.save(i)
with open("temp.xls", "w+b") as f:
    f.write(i.getvalue())

