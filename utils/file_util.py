import os
import openpyxl
from openpyxl.styles import Alignment

def num_to_title(n):
    res = ''
    while n > 0:
        res = (chr)((n-1)%26+65) + res
        n = int((n-1)/26)
    return res

def save_xlsx(file_name, content, tips=None):
    ex_file = openpyxl.Workbook()
    ex_file.create_sheet('Sheet1')
    table = ex_file.active

    #table.merge_cells(range_string='A1:Z1')
    #A1 = table['A1']
    #A1.value = tips
    #A1.alignment = Alignment(wrapText=True)
    #table.row_dimensions[1].height = 190

    lineidx = 1
    for idx in range(len(content)):
        colidx = 1
        for item in content[idx]:
            table.cell(lineidx, colidx, item)
            colidx += 1
        lineidx += 1

    for item in range(1, 100):
        if item == 1:
            width = 30
        else:
            width = 20
        table.column_dimensions[num_to_title(item)].width = width
    
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
    #print(get_files('/Users/tyree/Downloads/LH2207/4/2021', '_6_5'))
    print(num_to_title(27))
