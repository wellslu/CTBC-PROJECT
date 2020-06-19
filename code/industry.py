import pandas as pd

def indus_sort(data, date):
    pre_date = []
    date_list = sorted(list(set(data['Date'])))
    # last_date = int(date[-1])
    # # insert newest date
    # if last_date % 4 == 0:
    #     date_list.append(str(last_date+100-12+3))
    # else:
    #     date_list.append(str(last_date+3))
    # # get pre 4 seasons date
    index = date_list.index(date)
    for r in range(6):# season
        if index >= 0:
            pre_date.append(date_list[index])
            index-=1
        else:
            break
    data = data[data['Date'].isin(pre_date)]
    data2 = data[data['gsector'].notnull()]
    data4 = data[data['ggroup'].notnull()]
    
    dic2 = {}
    for each_gsector in list(data2['gsector']):
        if each_gsector not in dic2:
            dic2[each_gsector] = 0
        dic2[each_gsector]+=1
    dic2 = {k: v for k, v in sorted(dic2.items(), reverse=True, key=lambda item: item[1])}
    for num in dic2:
        dic2[num] = (dic2[num] / len(list(data2[data2['gsector'].notnull()]['gsector'])))*100
    dic2_new = {}
    percent = 0
    dic2_key = list(dic2.keys())
    for num in dic2_key:
        percent = percent + dic2[num]
        dic2_new[num] = dic2[num]
        if percent > 80:
            break
    if len(dic2_new) < 5:
        for n in list(dic2)[:5]:
            dic2_new[n] = dic2[n]
    
    dic4 = {}
    for each_ggroup in list(data4['ggroup']):
        if each_ggroup not in dic4:
            dic4[each_ggroup] = 0
        dic4[each_ggroup]+=1
    dic4 = {k: v for k, v in sorted(dic4.items(), reverse=True, key=lambda item: item[1])}
    for num in dic4:
        dic4[num] = (dic4[num] / len(list(data4[data4['ggroup'].notnull()]['ggroup'])))*100
    dic4_new = {}
    dic4_key = list(dic4.keys())
    for num in dic4_key:
        if dic4[num] > 5:
            dic4_new[num] = dic4[num]
        else:
            break
    
    return pre_date, dic2_new, dic4_new