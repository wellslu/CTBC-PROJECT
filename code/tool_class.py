import pandas as pd

class Tool:
    @staticmethod
    def crawler(url):
        from crawler import crawler
        f13_data = crawler(url)
        return f13_data
    
    @staticmethod
    def pre_processing(f13_data, crsp_data):
        import pre_processing as pp
        f13_data_all = pp.Pre_Processing().launch(f13_data, crsp_data)
        return f13_data_all
    
    @staticmethod
    def industry_sort(data, date):
        from industry import indus_sort
        season_date_list,gsector_sort, ggroup_sort = indus_sort(data, date)
        return season_date_list,gsector_sort, ggroup_sort
    
    @staticmethod
    def company_combine(industry, beta, asset, stock):
        import company_combine as cc
        company = cc.Company_Combine().launch(industry, beta, asset, stock)
        return company
    
    @staticmethod
    def industry_filter(company, gsector_sort):
        company = company[company['gsector'].isin(list(gsector_sort.keys()))].reset_index(drop=True)
        return company
        
    @staticmethod
    def cma_hml(unbuy_tic):
        unbuy_tic = unbuy_tic.sort_values('book/price').reset_index(drop=True)
        unbuy_tic = unbuy_tic[:int(len(unbuy_tic)/2)]
        unbuy_tic = unbuy_tic.sort_values('investment', ascending=True).reset_index(drop=True)
        unbuy_tic = unbuy_tic[:int(len(unbuy_tic)/2)]
        return unbuy_tic