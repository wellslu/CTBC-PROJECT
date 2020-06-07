lone_pine_all.drop(['Class_x', 'price'], axis=1, inplace=True)
lone_pine_all.columns = ['Ticker', 'Date', 'Shares', 'Value', 'Shares_Change', 'Value_Change', 'Price',
                         'Class', 'Last_Price']

new_share = lone_pine_all.loc[lone_pine_all['Shares']==lone_pine_all['Shares_Change']]
new_share.loc[:,'New_Value'] = new_share['Shares'] * new_share['Price']

# new_share.to_csv('new_share.csv', index=False)

sell_share = lone_pine_all[lone_pine_all['Shares']==0]
sell_share.loc[:,'New_Value'] = sell_share['Shares_Change'] * sell_share['Price']

# sell_share.to_csv('sell_share.csv', index=False)

more_share = lone_pine_all[lone_pine_all['Shares_Change']>0].loc[lone_pine_all['Shares']!=lone_pine_all['Shares_Change']]
more_share.loc[:,'New_Value'] = more_share['Shares'] * more_share['Price']

# more_share.to_csv('more_share.csv', index=False)

less_share = lone_pine_all[lone_pine_all['Shares_Change']<0].loc[lone_pine_all['Shares']!=0]
less_share.loc[:,'New_Value'] = (less_share['Shares'] + abs(less_share['Shares_Change'])) * less_share['Price']

# less_share.to_csv('less_share.csv', index=False)

same_share = lone_pine_all[lone_pine_all['Shares_Change']==0]
same_share.loc[:,'New_Value'] = same_share['Shares'] * same_share['Price']

# same_share.to_csv('same_share.csv', index=False)

lone_pine_all.loc[:,'New_Value'] = lone_pine_all['Shares'] * lone_pine_all['Price']

total_asset = []
for date in list(set(lone_pine_all['Date'])):
    lone_pine_all_t = lone_pine_all[lone_pine_all['Date']==date]
    total_asset.append(sum(lone_pine_all_t['New_Value']))

revenue = []
for date in list(set(lone_pine_all['Date'])):
    lone_pine_all_t = lone_pine_all[lone_pine_all['Date']==date]

    sell_shares = lone_pine_all_t[lone_pine_all_t['Shares']==0]
    more_shares = lone_pine_all_t[lone_pine_all_t['Shares_Change']>0].loc[lone_pine_all_t['Shares']!=lone_pine_all_t['Shares_Change']]
    less_shares = lone_pine_all_t[lone_pine_all_t['Shares_Change']<0].loc[lone_pine_all_t['Shares']!=0]
    same_shares = lone_pine_all_t[lone_pine_all_t['Shares_Change']==0]

    sell_shares.loc[:,'diff'] = abs(sell_shares['Shares_Change']) * ((sell_shares['Price'] - sell_shares['Last_Price']) / 2)
    more_shares.loc[:,'diff'] = (more_shares['Shares'] - more_shares['Shares_Change']) * (more_shares['Price'] - more_shares['Last_Price'])
    less_shares.loc[:,'diff'] = less_shares['Shares'] * (less_shares['Price'] - less_shares['Last_Price']) + abs(less_shares['Shares_Change']) * ((less_shares['Price'] - less_shares['Last_Price']) / 2)
    same_shares.loc[:,'diff'] = same_shares['Shares'] * (same_shares['Price'] - same_shares['Last_Price'])
    
    sell_shares = sell_shares[sell_shares['diff'].notnull()]
    more_shares = more_shares[more_shares['diff'].notnull()]
    less_shares = less_shares[less_shares['diff'].notnull()]
    same_shares = same_shares[same_shares['diff'].notnull()]
    
    revenue.append(sum(sell_shares['diff']) + sum(more_shares['diff']) + sum(less_shares['diff']) + sum(same_shares['diff']))

revenue_df = pd.DataFrame({'date':list(set(lone_pine_all['Date'])),
                          'asset':total_asset,
                          'revenue':revenue})

revenue_df = revenue_df.sort_values(by='date').reset_index(drop=True)

revenue_df['last_asset'] = revenue_df['asset'].shift()

revenue_df['return_rate'] = round(revenue_df['revenue'] / revenue_df['last_asset'] * 100, 4)

revenue_df = revenue_df[0:-2].reset_index(drop=True)

revenue_df

revenue_df.to_csv('revenue.csv', index=False)