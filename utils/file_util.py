import os
import openpyxl
from openpyxl.styles import Alignment

def save_xlsx(file_name, content, tips=None):
    ex_file = openpyxl.Workbook()
    ex_file.create_sheet('Sheet1')
    table = ex_file.active

    table.merge_cells(range_string='A1:Z1')
    #table.cell(1, 1, '顾比策略: \n 位置1: boll_st_s1是否大于boll_st_s5（1:小于, 2:大于, 0:等于）\n 位置2: boll_st_s1是否下穿boll_st_s5（1: 下穿, 0: 未下穿）')
    A1 = table['A1']
    A1.value = tips
    A1.alignment = Alignment(wrapText=True)
    table.row_dimensions[1].height = 190

    lineidx = 2
    for idx in range(len(content)):
        colidx = 1
        for item in content[idx]:
            table.cell(lineidx, colidx, item)
            colidx += 1
        lineidx += 1

    for item in range(65, 91):
        if item == 65:
            width = 30
        else:
            width = 20
        table.column_dimensions[chr(item)].width = width
    
    ex_file.save(file_name)

def get_files(path, filters):
    res = []
    temp = os.listdir(path)
    for t in temp:
        if (os.path.isfile(path + '/' + t)):
            if filters in t:
                res.append(path + '/' + t);
        else:
            res.extend(get_files(path + '/' + t, filters))
    return res

if __name__ == '__main__':
    print(get_files('/Users/tyree/Downloads/LH2207/4/2021', '_6_5'))
