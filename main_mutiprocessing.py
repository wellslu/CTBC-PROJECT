import multiprocessing as mp
from function import test_fun

if __name__ == '__main__':
	a_date = []
	for i in range(200501, 201913, 1):
	    i = str(i)
	    if 1 <= int(i[4:]) <= 12:
	        a_date.append(i)
	
    pool_list = [i for i in range(0, len(a_date)-1, 1)]
    pool = mp.Pool(processes=5)
    df_list = pool.map(test_fun.fun, pool_list)
