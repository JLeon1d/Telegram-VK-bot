import config
import logging # для отслеживания логов ошибок
import vk_api
import time
import pandas as pd # для работы с датафреймами
import apscheduler # для планирования задач
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup

from vk_api.longpoll import VkLongPoll, VkEventType
from aiogram import Bot, Dispatcher, executor, types

logging.basicConfig(level=logging.INFO)

bot = Bot(token=config.token_tg) # подключаем бота в Телеграмме
dp = Dispatcher(bot)

session = vk_api.VkApi(token=config.token_vk)
vk = session.get_api()

tools = vk_api.VkTools(session)

df = pd.read_csv("base.csv")# загружаем базу данных из файла .csv в датафрейм df
last_time_updated = time.time()

def save_base(df) : # для обновления базы данных
    df.to_csv("base.csv", index=None)

def clear_base(df) : # для очистки базы данных
    df = pd.DataFrame({'id' : [], 'cnt' : [], 'walls' : [], 'last_command' : []})
    df.to_csv("base.csv", index=None)
    return df

#df = clear_base(df)

def is_correct_link(text) : # проверка ссылки на корректность
    for i in range(len(text)) :
        if text[i] == " " :
            text = text[i + 1:len(text)]
            break
    if len(text) < 7  or (text[0:7] != "vk.com/" and (len(text) < 15 or text[0:15] != "https://vk.com/")) :
        return False
    return True

def get_group_name(text) : # получение имени группы/человека по ссылке
    for i in range(len(text)) :
        if text[i] == " " :
            text = text[i + 1:len(text)]
            break
    if text[0:3] != "vk." :
        #https://
        text = text[8:len(text)]
    #vk.com/
    return text[7:len(text)]

def get_group_id(text) : # получение id группы 
    for i in range(len(text)) :
        if text[i] == " " :
            text = text[i + 1:len(text)]
            break
    return text

def merge_groups(groups) : # объединение групп в 1 строку
    if len(groups) == 0 :
        return ""
    s = groups[0]
    for i in range(1, len(groups)) :
        s = s + " " + groups[i]
    return s

def is_new_user(user_id) : # проверяем есть ли пользователь в базе данных
    global df
    if len(df.loc[df['id'] == user_id]) == 0 :
        df.loc[len(df)] = [user_id, 0, str(""), "-"]
        save_base(df)

async def get_name(name : str) : # получаем имя группы/человека в ВК по из короткому имени
    what_type = session.method("utils.resolveScreenName", {"screen_name" : name})
    if what_type['type'] == 'user' :
        user = session.method("users.get", {"user_ids" : name})
        return str(user[0]['first_name'] + " " + user[0]['last_name'])
    else :
        group = session.method('groups.getById', {"group_id" : name})
        return group[0]['name']

async def send_post(post, user_id) : # отправка поста
    media = types.MediaGroup()
    if 'text' in post and len(post['text']) != 0 :
        await bot.send_message(user_id, post['text'])
    if 'attachments' in post:
        for item in post['attachments']:
            if 'photo' in item:
                media.attach_photo(item['photo']['sizes'][len(item['photo']['sizes']) - 1]['url'])
            if 'video' in item :
                video_link = "https://vk.com/video" + str(item['video']['owner_id']) + "_" + str(item['video']['id'])
                await bot.send_message(user_id, "<i>Если бы ВК позволяло, то здесь было бы видео</i>\n" + video_link, parse_mode=types.ParseMode.HTML)
            if 'audio' in item :
                await bot.send_message(user_id, "<i>Если бы ВК позволяло, то здесь бы была песня</i>\n" + item['audio']['artist'] + " - " + item['audio']['title'], parse_mode=types.ParseMode.HTML)
                print(item['audio'])
            if 'doc' in item :
                media.attach_document(item['doc']['url'])
            if 'poll' in item :
                poll = "<b>" + item['poll']['question'] + "(Опрос)</b>\n"
                num = int(1)
                for ans in item['poll']['answers'] :
                    poll += str(num) + '.' + ans['text'] + " - <b>" + str(ans['votes']) + "(" + str(ans['rate']) + "%)</b>\n"
                    num += 1
                poll += "<i>Всего голосов: " + str(item['poll']['votes']) + "</i>"
                await bot.send_message(user_id, poll, parse_mode=types.ParseMode.HTML)
    
    if len(media.media) != 0 :
        await bot.send_media_group(user_id, media=media)

async def send_last_post(user_id, name) : # отправка последнего поста
    wall = session.method('wall.get', {"domain": name, "count": 2})
    if len(wall['items']) == 0 :
        await bot.send_message(user_id, "Здесь ещё нет постов(")
        return
    post = wall['items'][0]
    if len(wall['items']) > 1 and wall['items'][1]['date'] > wall['items'][0]['date'] :
        post = wall['items'][1]
    await send_post(post, user_id)

async def add_command(message: types.Message) : # добавление подписки
    global df
    user_id = int(message.from_user.id)
    if is_correct_link(message.text) == False :
        await bot.send_message(user_id, "Неправильная ссылка!")
        return
    group = get_group_name(message.text)
    is_existing_group = session.method("utils.resolveScreenName", {"screen_name": group})
    if len(is_existing_group) == 0 :
        await bot.send_message(user_id, "Такого человека/сообщества не существует(")
        return
    s = str(df.loc[df['id'] == user_id, 'walls'].tolist()[0])
    groups = list(s.split())
    if group in groups :
        await bot.send_message(user_id, "Вы уже подписаны(")
        return
    what_type = session.method("utils.resolveScreenName", {"screen_name": group})
    can_add = True
    if len(what_type) == 0 or what_type['type'] == "application":
        await bot.send_message(user_id, "Неправильная ссылка!")
        return
    if what_type['type'] == "user":
        is_closed = session.method("users.get", {"user_ids": group})
        if is_closed[0]['first_name'] == 'DELETED' or is_closed[0]['is_closed'] == True:
            await bot.send_message(user_id, "Не могу добавить подписку,\nПрофиль удалён или защищён настройками приватности!")
            can_add = False
    else :
        Group = session.method("groups.getById", {"group_id": group})
        if Group[0]['is_closed'] != 0 :
            await bot.send_message(user_id, "Не могу добавить подписку,\nЗакрытая группа!")
            return
    if can_add :
        cnt = int(df.loc[df['id'] == user_id, 'cnt'].tolist()[0])
        if cnt == 5 :
            await bot.send_message(user_id, "Достигнут лимит групп!")
        else :  
            if len(s) == 0 :
                s = str(group)
            else :
                s = s + " " + str(group)
            df.loc[df['id'] == user_id] = [user_id, cnt + 1, s, "-"]
            await bot.send_message(user_id, "Новая подписка была добавлена!")

async def delete_group(user_id, group_id) : # удаление подписки
    global df
    s = str(df.loc[df['id'] == user_id, 'walls'].tolist()[0])
    groups = list(s.split())
    cnt = int(df.loc[df['id'] == user_id, 'cnt'].tolist()[0])
    groups.pop(group_id)
    s = merge_groups(groups)
    df.loc[df['id'] == user_id] = [user_id, cnt - 1, s, "-"]

@dp.message_handler(commands=['help'])
async def process_help(message: types.Message):
    global df
    is_new_user(message.from_user.id)
    c1 = "/start - Просто старт бота\n"
    c2 = "/help - Список комманд\n"
    c3 = "/add - Добавить подписку на группу/человека\n"
    c4 = "/delete - Удалить подписку на группу/человека\n"
    c5 = "/list - Список всех подписок\n"
    c6 = "/last - последний пост группы/человека\n"
    await bot.send_message(message.from_user.id, "Вот что я умею:\n" + c1 + c2 + c3 + c4 + c5 + c6)
    df.at[int(df.loc[df["id"] == message.from_user.id].index.values[0]), 'last_command'] = "-"

@dp.message_handler(commands=['start'])
async def process_start(message: types.Message):
    global df
    user_id = message.from_user.id
    await bot.send_message(user_id, "Привет!\nЯ бот, с помощью которого можно следить за интересующими вас группами/людьми в Вконтакте.\nЯ буду присылать новые посты прямо в Телеграм!\nИспользуй /help чтобы подробнее ознакомиться с моим функционалом.")
    is_new_user(user_id)
    df.at[int(df.loc[df["id"] == user_id].index.values[0]), 'last_command'] = "-"

@dp.message_handler(commands=['add'])
async def process_add_new_wall(message: types.Message):
    global df
    user_id = int(message.from_user.id)
    is_new_user(user_id)
    df.at[int(df.loc[df["id"] == user_id].index.values[0]), 'last_command'] = "add"
    await bot.send_message(user_id, "Пришлите мне ссылку на группу/человека для подписки")

@dp.message_handler(commands=['delete'])
async def process_delete_wall(message: types.Message):
    global df
    user_id = int(message.from_user.id)
    is_new_user(user_id)
    markup = InlineKeyboardMarkup()
    markup.row_width = 1
    if int(df.loc[df['id'] == user_id, 'cnt'].tolist()[0]) == 0 :
        await bot.send_message(user_id, "Ваш список подписок пуст(")
        return
    # долго создаётся
    groups = list(str(df.loc[df['id'] == user_id, 'walls'].tolist()[0]).split())
    for i in range(len(groups)) :
        group_name = await get_name(groups[i])
        markup.add(InlineKeyboardButton(group_name, callback_data=str("1 " + str(i))))
    await bot.send_message(user_id, "Выберите группу которую хотите удалить:", reply_markup=markup)
    df.at[int(df.loc[df["id"] == user_id].index.values[0]), 'last_command'] = "delete"

@dp.callback_query_handler(lambda call: True) # хэндлер кнопочек под сообщением
async def buttons_answer(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    user_id = callback_query.from_user.id
    what_to_do = int(callback_query.data.split()[0])
    group_id = int(callback_query.data.split()[1])
    message_id = callback_query.message.message_id
    chat_id = callback_query.message.chat.id
    await bot.edit_message_reply_markup(chat_id=chat_id, message_id = message_id, reply_markup = '')
    if what_to_do == 0 :
        name = list(str(df.loc[df['id'] == user_id, 'walls'].tolist()[0]).split())[group_id]
        await bot.edit_message_text(chat_id=chat_id, message_id = message_id, text=str("<b>" + await get_name(name) + ":</b>"), parse_mode=types.ParseMode.HTML)
        await send_last_post(user_id, name)
    else :
        await bot.edit_message_text(chat_id=chat_id, message_id = message_id, text="Подписка удалена.")
        await delete_group(user_id, group_id)

@dp.message_handler(commands=['last'])
async def process_last_post(message: types.Message):
    global df
    user_id = int(message.from_user.id)
    is_new_user(user_id)
    df.at[int(df.loc[df["id"] == user_id].index.values[0]), 'last_command'] = "-"
    markup = InlineKeyboardMarkup()
    markup.row_width = 1
    if int(df.loc[df['id'] == user_id, 'cnt'].tolist()[0]) == 0 :
        await bot.send_message(user_id, "Ваш список подписок пуст(")
        return
    #Очень долго создаётся
    groups = list(str(df.loc[df['id'] == user_id, 'walls'].tolist()[0]).split())
    for i in range(len(groups)) :
        group_name = await get_name(groups[i])
        markup.add(InlineKeyboardButton(group_name, callback_data=str("0 " + str(i))))
    await bot.send_message(user_id, "Выберите группу:", reply_markup=markup)

@dp.message_handler(commands=['list'])
async def process_list_of_subscriptions(message: types.Message):
    global df
    user_id = message.from_user.id
    is_new_user(user_id)
    answer = "Ваши подписки:\n"
    if int(df.loc[df['id'] == user_id, 'cnt'].tolist()[0]) != 0 :
        groups = list(str(df.loc[df['id'] == user_id, 'walls'].tolist()[0]).split())
        for i in range(len(groups)):
            answer += str(i + 1) + ". "
            answer += await get_name(groups[i]) + "("
            answer += "vk.com/" + groups[i] + ")" + "\n"
        await bot.send_message(user_id, answer)
    else :
        await bot.send_message(user_id, "У вас нет подписок(\n Используйте /add")
    df.at[int(df.loc[df["id"] == user_id].index.values[0]), 'last_command'] = "-"
    
@dp.message_handler()
async def all_messages(message: types.Message):
    global df
    user_id = int(message.from_user.id)
    is_new_user(user_id)
    command = str(df.loc[df["id"] == user_id, 'last_command'].tolist()[0])
    print(command)
    if command == "add" :
        await add_command(message)
    else :
        await bot.send_message(user_id, "Некорректная команда(\nНапиши /help для справки.")
    df.at[int(df.loc[df["id"] == user_id].index.values[0]), 'last_command'] = "-"

async def new_posts_check() : # проверка на наличие новых постов в группе
    global df
    global last_time_updated
    for i in range(len(df)) :
        user = df.loc[i]
        user_id = user['id']
        groups = list(str(user['walls']).split())
        for domain in groups :
            wall = session.method('wall.get', {"domain": domain, "count": 2})
            group_name = await get_name(domain)
            if len(wall['items']) == 0 :
                break
            for post in wall['items'] :
                if post['date'] > last_time_updated :
                    await bot.send_message(user_id, str("<b>" + group_name + ":</b>"), parse_mode=types.ParseMode.HTML)
                    await send_post(post, user_id)
    last_time_updated = time.time()
    save_base(df)
    print("New posts check ended!")

if __name__ == '__main__':
    scheduler = AsyncIOScheduler()
    scheduler.add_job(new_posts_check, 'interval', seconds=config.UPDATE_GAP) # запускаем проверку стен на новые посты каждые UPDATE_GAP секунд
    scheduler.start()
    executor.start_polling(dp, skip_updates=True)
