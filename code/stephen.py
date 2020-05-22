#!/usr/bin/env python
# coding: utf-8

# In[47]:


import telegram_send
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, Filters, MessageHandler
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
import pandas as pd
import numpy as np

revenue = pd.read_csv('revenue.csv')
revenue['date'] = revenue['date'] // 100

def start(bot, update):
    update.message.reply_text('你好，我是史蒂芬・曼德爾，你想問啥?',
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton('個人資料', callback_data = 'introduce')],
            [InlineKeyboardButton('近三年報酬率', callback_data = 'return_rate')],
            [InlineKeyboardButton('投資組合', callback_data = 'portfolio')]]))


def introduce(bot, update):
    update.callback_query.edit_message_text('''我的年齡:64\n創立機構:lone pine\n身價:28億美元''')
    updater.dispatcher.add_handler(CommandHandler(start))

def return_rate(bot, update):
    global revenue
    years = sorted(list(set(revenue['date'])))
    first = years[-1]
    second = years[-2]
    third = years[-3]
    first_return_rate = np.mean(revenue[revenue['date']==first]['return_rate'])
    second_return_rate = np.mean(revenue[revenue['date']==second]['return_rate'])
    third_return_rate = np.mean(revenue[revenue['date']==third]['return_rate'])
    update.callback_query.edit_message_text(str(first)+'平均報酬率 : '+str(round(first_return_rate,3))+'%\n'+str(second)+'平均報酬率 : '+str(round(second_return_rate,3))+'%\n'+str(third)+'平均報酬率 : '+str(round(third_return_rate,3))+'%')
    
def portfolio(bot, update):
    update.callback_query.edit_message_text('此功能尚未開啟，有種花錢解鎖啊!')
    
def reply_handler(bot, update):
    enter = update.message.text
    update.message.reply_text('沒叫你亂自己輸入')


updater = Updater('1208935186:AAFOhiuSdlSbCdlZLU52B5HODMYm4Wkhmls')

updater.dispatcher.add_handler(CommandHandler('start', start))
updater.dispatcher.add_handler(CallbackQueryHandler(introduce, pattern='introduce'))
updater.dispatcher.add_handler(CallbackQueryHandler(return_rate, pattern='return_rate'))
updater.dispatcher.add_handler(CallbackQueryHandler(portfolio, pattern='portfolio'))
updater.dispatcher.add_handler(MessageHandler(Filters.text, reply_handler))


updater.start_polling()
updater.idle()


# In[ ]:




