import telegram_send
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, Filters, MessageHandler
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
import pandas as pd
import numpy as np
import logging

# # Set up basic logging
# logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#                     level=logging.INFO)

pred_portfolio = pd.read_csv('pred_portfolio.csv')
pred_portfolio['class'] = pred_portfolio['class'].replace('0', '')
pred_portfolio_t = pd.DataFrame()

def start(bot, update):
    update.message.reply_text('你好，我是史蒂芬・曼德爾，你想問啥?',
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton('個人資料', callback_data = 'introduce')],
            [InlineKeyboardButton('投資組合', callback_data = 'portfolio')],
            [InlineKeyboardButton('每月報酬線圖', callback_data = 'revenue')],
            [InlineKeyboardButton('投資產業比例', callback_data = 'industry')]]))

def introduce(bot, update):
    bot.edit_message_text('''年齡:64歲\n創立機構:lone pine\n身價:28億美元\n從原本募資800萬美元的資金，目前已躍昇為管理230億美元的大型基金''',
                         chat_id=update.callback_query.message.chat_id,
                        message_id=update.callback_query.message.message_id)
    bot.answer_callback_query(update.callback_query.id, text='')
    
def portfolio(bot, update):
    bot.edit_message_text('輸入日期',
                        chat_id=update.callback_query.message.chat_id,
                        message_id=update.callback_query.message.message_id,
                         reply_markup = InlineKeyboardMarkup([
                        [InlineKeyboardButton('201909', callback_data = '201909')],
                        [InlineKeyboardButton('201910', callback_data = '201910')],
                        [InlineKeyboardButton('201911', callback_data = '201911')],
                        [InlineKeyboardButton('201912', callback_data = '201912')]]))
    bot.answer_callback_query(update.callback_query.id, text='')

def industry(bot, update):
    chat_id = update.callback_query.message.chat.id
    bot.send_photo(chat_id=chat_id, photo=open('industry_pie.png', 'rb'))

def revenue(bot, update):
    chat_id = update.callback_query.message.chat.id
    bot.send_photo(chat_id=chat_id, photo=open('revenue_plot.png', 'rb'))

def recommend(bot, update):
    global pred_portfolio
    global pred_portfolio_t
    enter = update.callback_query.data
    pred_portfolio_t = pred_portfolio[pred_portfolio['date']==int(enter)]
#     pred_portfolio_t.drop('date', axis=1, inplace=True)
#     pred_portfolio_t = pred_portfolio_t[['class', 'ticker']]
    pred_portfolio_t['ticker'] = pred_portfolio_t['ticker'] + ' ' + pred_portfolio_t['class']
    bot.edit_message_text('選擇價格區間',
                        chat_id=update.callback_query.message.chat_id,
                        message_id=update.callback_query.message.message_id,
                         reply_markup = InlineKeyboardMarkup([
                        [InlineKeyboardButton('0~30', callback_data = '0~30')],
                        [InlineKeyboardButton('30~60', callback_data = '30~60')],
                        [InlineKeyboardButton('60~100', callback_data = '60~100')],
                        [InlineKeyboardButton('100~200', callback_data = '100~200')],
                        [InlineKeyboardButton('200~', callback_data = '200~')]]))
    bot.answer_callback_query(update.callback_query.id, text='')

def recommend_portfolio(bot, update):
    global pred_portfolio_t
    enter = update.callback_query.data
    if enter == '0~30':
        pred_portfolio_t = pred_portfolio_t[pred_portfolio_t['PRC']<30]
    elif enter == '30~60':
        pred_portfolio_t = pred_portfolio_t[pred_portfolio_t['PRC']>=30].loc[pred_portfolio_t['PRC']<60]
    elif enter == '60~100':
        pred_portfolio_t = pred_portfolio_t[pred_portfolio_t['PRC']>=60].loc[pred_portfolio_t['PRC']<100]
    elif enter == '100~200':
        pred_portfolio_t = pred_portfolio_t[pred_portfolio_t['PRC']>=100].loc[pred_portfolio_t['PRC']<200]
    else:
        pred_portfolio_t = pred_portfolio_t[pred_portfolio_t['PRC']>=200]
    bot.edit_message_text('推薦組合\n'+pred_portfolio_t[['ticker']].to_string(index = False),
                        chat_id=update.callback_query.message.chat_id,
                        message_id=update.callback_query.message.message_id,)
    bot.answer_callback_query(update.callback_query.id, text='')

def reply_handler(bot, update):
    enter = update.message.text
    update.message.reply_text('沒叫你亂自己輸入',
                             chat_id=update.callback_query.message.chat_id,
                        message_id=update.callback_query.message.message_id)

updater = Updater('1208935186:AAFOhiuSdlSbCdlZLU52B5HODMYm4Wkhmls')

updater.dispatcher.add_handler(CommandHandler('start', start))
updater.dispatcher.add_handler(CallbackQueryHandler(introduce, pattern='introduce'))
updater.dispatcher.add_handler(CallbackQueryHandler(portfolio, pattern='portfolio'))
updater.dispatcher.add_handler(CallbackQueryHandler(revenue, pattern='revenue'))
updater.dispatcher.add_handler(CallbackQueryHandler(industry, pattern='industry'))
updater.dispatcher.add_handler(CallbackQueryHandler(recommend, pattern='201909'))
updater.dispatcher.add_handler(CallbackQueryHandler(recommend, pattern='201910'))
updater.dispatcher.add_handler(CallbackQueryHandler(recommend, pattern='201911'))
updater.dispatcher.add_handler(CallbackQueryHandler(recommend, pattern='201912'))
updater.dispatcher.add_handler(CallbackQueryHandler(recommend_portfolio, pattern='0~30'))
updater.dispatcher.add_handler(CallbackQueryHandler(recommend_portfolio, pattern='30~60'))
updater.dispatcher.add_handler(CallbackQueryHandler(recommend_portfolio, pattern='60~100'))
updater.dispatcher.add_handler(CallbackQueryHandler(recommend_portfolio, pattern='100~200'))
updater.dispatcher.add_handler(CallbackQueryHandler(recommend_portfolio, pattern='200~'))

updater.dispatcher.add_handler(MessageHandler(Filters.text, reply_handler))

updater.start_polling()
updater.idle()