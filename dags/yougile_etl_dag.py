import pandas as pd
import numpy as np
import requests
import json
import time
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.hooks.postgres_hook import PostgresHook

#получаем необходимые для работы перменные
TOKEN_BOT_TG = Variable.get("TOKEN_BOT_TG")
CHAT_ID_TG = Variable.get("CHAT_ID_TG")
POSTGRES_CONN_ID = 'postgres_conn'
BASE_URL_YG = Variable.get("BASE_URL_YG")
OAUTH_TOKEN_YG = Variable.get("OAUTH_TOKEN_YG")
ACTUAL_BOARD_NAMES = Variable.get("ACTUAL_BOARD_NAMES", deserialize_json=True)

HEADERS = {'Content-Type': 'application/json',
           'Authorization': f'Bearer {OAUTH_TOKEN_YG}'}

#входные аргуемнты для DAG
args = {'owner': 'service',
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2}

#функция отправляет в телеграм бот сообщение об ошибке во время выполнения DAG
def on_failure_callback(context):
    send_message = TelegramOperator(task_id='send_message_to_telegram',
                                    telegram_conn_id='tg_notify_bot',
                                    text=f"\U0000274C DAG <b>{context['ti'].dag_id}</b>"f" is finished with error in task {context['ti'].task_id}")
    return send_message.execute(context=context)

#функция отправки уведомления в телеграм бот с заданным текстом
def send_message_tg(text):
    url = f'https://api.telegram.org/bot%s/sendMessage' %TOKEN_BOT_TG
    data = {'chat_id': CHAT_ID_TG, 'text': text}
    r = requests.post(url, data=data)
    return r

#функция получения сырых данных из YouGile
def get_yg_data(method, offset, limit, include_deleted, column_id):
    #параметры запроса
    params = f'?offset={offset}&limit={limit}&includeDeleted={include_deleted}'
    #если задано айди колонки добавим параметр
    if column_id is not None:
        params = params + f'&columnId={column_id}'
    #сюда собираем результат
    full_data = []
    #параметр, чтобы стартануть цикл
    next = True
    #так как выгрзука данных может быть парционной релизован цикл
    while next is True:
        #получаем порцию данных
        response = json.loads(requests.get(BASE_URL_YG + method + params, headers=HEADERS).text)
        #добавляем в результат
        full_data = full_data + response['content']
        #получаем значение параметра next
        next = response['paging']['next']
        #формируем параметры для запроса следующей порции данных
        offset = offset + limit
        params = f'?limit={limit}&offset={offset}&includeDeleted={include_deleted}'
        if column_id is not None:
            params = params + f'&columnId={column_id}'
        if next is True:
            #соблюдаем ограничение - не более 50 запросов к api в минуту
            time.sleep(1.5)

    #возвращаем результат
    return full_data

#функция получения порции задач из YouGile
def get_df_tasks_portion(column_id, limit, include_deleted, actual_columns):
    #получаем задачи по переданным параметрам (сырой json)
    raw_yg_tasks = get_yg_data(method='tasks',
                               offset=0,
                               limit=limit,
                               include_deleted=include_deleted,
                               column_id=column_id)
    #преобразуем в датафрейм
    df_tasks_portion = pd.json_normalize(raw_yg_tasks)

    #если датасет не пустой, то приведем его к целевой структуре
    if len(df_tasks_portion) != 0:
        for column in actual_columns:
            if column not in df_tasks_portion.columns:
                df_tasks_portion[column] = None
        df_tasks_portion = df_tasks_portion[actual_columns]
    
    #возращаем результат
    return df_tasks_portion

#функция формирует справочник, который в себя включайт соответсвтие id состояния <-> значение состояние
#нужно для того, чтобы вместо айдишников в некоторых колонах получить сами значения (какой базис поставки, какой статус и тп)
def get_local_hub(df_global_hub, value, column_state_id, column_name):
    df_local_hub = (df_global_hub[df_global_hub['str_sticker_name'] == value].
                    rename(columns={'str_sticker_state_id': column_state_id,
                                    'str_sticker_state_name': column_name}).
                    reset_index(drop=True))[[column_state_id, column_name]]
    return df_local_hub

#функция получения данных датафрейма с колонками и досками в них из YouGile
def get_df_yg_brd_clmn(ti):
    #получаем данные о досках
    raw_yg_boards = get_yg_data(method='boards',
                                offset=0,
                                limit=1000,
                                include_deleted='false',
                                column_id=None)
    #выбираем и преобразовываем интересующие столбцы датасета
    df_yg_boards = pd.json_normalize(raw_yg_boards)[['title', 'projectId', 'id']]
    df_yg_boards.columns = ['board_name', 'project_id', 'board_id']

    #получаем данные о колонках
    raw_yg_columns = get_yg_data(method='columns',
                                 offset=0,
                                 limit=1000,
                                 include_deleted='false',
                                 column_id=None)

    #выбираем и преобразовываем интересующие столбцы датасета
    df_yg_columns = pd.json_normalize(raw_yg_columns)[['title', 'boardId', 'id']]
    df_yg_columns.columns = ['column_name', 'board_id', 'column_id']

    #соединим с данными о досках и колонках
    df_brd_clmn = df_yg_columns.merge(df_yg_boards, how='left')
    #нужно взять только те колонки, которые относятся к отслеживаемым доскам
    #отбираем данные по актуальным колонкам
    df_brd_clmn = df_brd_clmn.query('board_name in @ACTUAL_BOARD_NAMES').reset_index(drop=True)

    #пуш xcom полученного датафрейма в следующую задачу
    ti.xcom_push(key='df_brd_clmn', value=df_brd_clmn)

#функция получения датафрейма с текстовыми стикерами из YouGile
def get_df_yg_str_stickers(ti):
    #получаем данные о текстовых стикерах
    raw_yg_str_stickers = get_yg_data(method='string-stickers',
                                      offset=0,
                                      limit=1000,
                                      include_deleted='false',
                                      column_id=None)

    #отбираем нужные столбцы датаеста
    df_str_stickers = pd.json_normalize(raw_yg_str_stickers)[['id', 'name', 'states']]
    df_str_stickers.columns = ['str_sticker_id', 'str_sticker_name', 'str_sticker_states']
    #расшиваем датасет (возможные значени стикера в отдельные строки)
    df_str_stickers = df_str_stickers.explode('str_sticker_states')

    #дальше разошьем в цельный датасет (достаним нужные значения из str_sticker_states в отдельные столбцы)
    df_str_stickers['str_sticker_state_id'] = df_str_stickers['str_sticker_states'].apply(lambda x: x.get('id'))
    df_str_stickers['str_sticker_state_name'] = df_str_stickers['str_sticker_states'].apply(lambda x: x.get('name'))
    #удаляем лишнее
    df_str_stickers = df_str_stickers.drop(columns=['str_sticker_states'])

    #пуш xcom полученного датафрейма в следующую задачу
    ti.xcom_push(key='df_str_stickers', value=df_str_stickers)

#функция получения датафрейма с задачами и подзадачами из YouGile
def get_df_yg_tasks(ti):
    #берем переданный из get_df_brd_clmn() датафрейм с досками и колонками через xcom
    df_brd_clmn = ti.xcom_pull(key='df_brd_clmn')

    #(ключи - исходные наименования из yougile, значения - наимениования к которые будем преобразовывать)
    #словарь актуальных столбцов при выгрузке списка задач (сделки) из YouGile
    actual_columns_tasks_dict = {'title':'task_name',
                                 'timestamp': 'task_creation_dt',
                                 'id': 'task_id',
                                 'subtasks': 'subtasks',
                                 'columnId': 'column_id',
                                 'stickers.0b7061b6-9090-4180-892d-63bbd61be1f6': 'contract_status_state_id'}
    
    #словарь актуальных столбцов при выгрузке списка подзадач (лотов) из YouGile
    actual_columns_subtasks_dict = {'title':'subtask_name',
                                    'id': 'subtask_id',
                                    'deleted': 'deleted',
                                    'stickers.e18e09d0-9ec4-4a0b-900f-384743d78523': 'delivery_term_state_id',
                                    'stickers.051e30b3-10f2-41d6-9048-eba4ca91305e': 'lot_status_state_id',
                                    'stickers.deaf7ee5-0c49-4094-8de0-03eb02145509': 'loading_place_state_id',
                                    'stickers.873e111a-9fda-4f68-bc86-bc665f93ae9a': 'ship_name',
                                    'stickers.36fb83ef-0405-4b2a-8270-1de9ca29b7b1': 'quantity_plan',
                                    'stickers.e1ab96ff-2a02-4d6c-8004-89018d6dbb8d': 'quantity_fact', 
                                    'stickers.5a3032f5-0034-4f00-bd8d-adcd6d2017b9': 'discharging_place1',
                                    'stickers.d070097e-a896-4ae6-aa24-850002c1dd0e': 'discharging_place2',
                                    'stickers.9ffa1a09-8223-4d2b-8e02-afa16738c020': 'loading_dates1',
                                    'stickers.b3e74941-5539-457c-a3bf-5bb0324e0b14': 'loading_dates2',
                                    'stickers.2cb61db5-4d56-4ef1-8d74-b20027666dc3': 'prov_paid_state_id',
                                    'stickers.22f6682b-1390-40a7-9783-7dce1f911f22': 'final_paid_state_id',
                                    'deadline.startDate': 'deadline_start_date',
                                    'deadline.deadline': 'deadline_end_date'}

    #сюда будем собирать все задачи (сделки)
    df_tasks = pd.DataFrame(columns=list(actual_columns_tasks_dict.keys()))

    #будем собирать задачи (сделки) отдельно по каждой колонке yougile
    #(ps. был обнаружен баг на уровне работы api yougile - если выгрузать сразу целиком все задачи, то при использовании параметра offset в запросе
    #может потерятся задача/задачи. это критично - нужно исключить такой момент)
    for i in range(len(df_brd_clmn)):
        #берем айди колонки, из которой будем получать задачи (сделки)
        column_id = df_brd_clmn['column_id'][i]

        #получаем порцию задач (запрос к api yougile)
        df_i = get_df_tasks_portion(column_id=column_id, limit=1000, include_deleted='false', actual_columns=list(actual_columns_tasks_dict.keys()))
        
        #соблюдаем ограничение - не более 50 запросов к api в минуту
        time.sleep(1.5)
        #если задач в колонке нет - идем к следующей колонке
        if len(df_i) == 0:
            continue

        #добавим к результату
        df_tasks = pd.concat([df_tasks, df_i], ignore_index=True)

    #переименуем столбцы
    df_tasks = df_tasks.rename(columns=actual_columns_tasks_dict)
    #расшиваем датафрейм (внутри контракта - лоты поставки)
    df_tasks = df_tasks.explode('subtasks').rename(columns={'subtasks': 'subtask_id'})
    
    #собираем все подзадачи (лоты)
    df_subtasks = get_df_tasks_portion(column_id=None, limit=100, include_deleted='true', actual_columns=list(actual_columns_subtasks_dict.keys()))
    #переименуем столбцы
    df_subtasks = df_subtasks.rename(columns=actual_columns_subtasks_dict)
    
    #даты погрузки могут содержаться в одной из двух колонок - соберем их в одну
    df_subtasks['loading_dates'] = df_subtasks['loading_dates1'].fillna(df_subtasks['loading_dates2'])
    df_subtasks = df_subtasks.drop(columns=['loading_dates1', 'loading_dates2'])
    #место доставки может содержаться в одной из двух колонок - соберем их в одну
    df_subtasks['discharging_place'] = df_subtasks['discharging_place1'].fillna(df_subtasks['discharging_place2'])
    df_subtasks = df_subtasks.drop(columns=['discharging_place1', 'discharging_place2'])

    #через левое соединение к рабочему датасету обоготим его данными о лотах (подзадачах)
    df_tasks = df_tasks.merge(df_subtasks, how='left')
    #чистим от удаленных подзадач
    df_tasks = df_tasks[df_tasks['deleted'] != True].reset_index(drop=True)

    #проверка - если есть такие строки, где заполнено subtask_id, но при этом subtask_name пустое, то это значит
    #что какие-то подзадачи не выгрузились - бьем аларм, в котором надо разбираться
    if len(df_tasks[df_tasks['subtask_id'].notna() & df_tasks['subtask_name'].isna()]) > 0:
        send_message_tg('\U0000274C some data is lost in uploading from YouGile!')
    
    #пуш xcom полученного датафрейма в следующую задачу
    ti.xcom_push(key='df_tasks', value=df_tasks)

def prepare_result_df_for_insert(execution_date, ti):
    
    #берем полученные датафреймы из xcom
    df_brd_clmn = ti.xcom_pull(key='df_brd_clmn')
    df_str_stickers = ti.xcom_pull(key='df_str_stickers')
    df_tasks = ti.xcom_pull(key='df_tasks')

    #справочник статусы сделки
    df_contract_status_hub = get_local_hub(df_str_stickers, 'Статус сделки', 'contract_status_state_id', 'task_status')
    #справочник базисы поставки
    df_delivery_term_state_hub = get_local_hub(df_str_stickers, 'Базис поставки', 'delivery_term_state_id', 'delivery_term')
    #справочник статусы лотов
    df_lot_status_state_hub = get_local_hub(df_str_stickers, 'Статус лота', 'lot_status_state_id', 'subtask_status')
    #справочник места погрузки
    df_loading_place_state_hub = get_local_hub(df_str_stickers, 'Место погрузки', 'loading_place_state_id', 'loading_place')
    #справочник состояния предварительной оплаты
    df_prov_paid_state_hub = get_local_hub(df_str_stickers, 'Provisional paid', 'prov_paid_state_id', 'prov_paid')
    #справочник состояния финальной оплаты
    df_final_paid_state_hub = get_local_hub(df_str_stickers, 'Final paid', 'final_paid_state_id', 'final_paid')

    #собираем результат
    df = (df_tasks.
          merge(df_brd_clmn, how='left').
          merge(df_contract_status_hub, how='left').
          merge(df_delivery_term_state_hub, how='left').
          merge(df_lot_status_state_hub, how='left').
          merge(df_loading_place_state_hub, how='left').
          merge(df_prov_paid_state_hub, how='left').
          merge(df_final_paid_state_hub, how='left'))

    #сохраняем момент сборки витрины
    df['loaded_ts'] = pd.to_datetime(execution_date)

    #расшиваем даты погрузки на отдельные столбцы
    df['loading_dates'] = df['loading_dates'].str.replace(' ', '')
    df[['loading_start_date', 'loading_end_date']] = df['loading_dates'].str.split('-', expand=True)

    #приведем к типам данных
    #даты
    df['loading_start_date'] = pd.to_datetime(df['loading_start_date'], format='%d.%m.%Y').dt.floor('d')
    df['loading_end_date'] = pd.to_datetime(df['loading_end_date'], format='%d.%m.%Y').dt.floor('d')
    df['task_creation_dt'] = pd.to_datetime(df['task_creation_dt'], unit='ms').dt.floor('d')
    df['deadline_start_date'] = pd.to_datetime(df['deadline_start_date'], unit='ms').dt.floor('d')
    df['deadline_end_date'] = pd.to_datetime(df['deadline_end_date'], unit='ms').dt.floor('d')
    #числа
    df['quantity_plan'] = df['quantity_plan'].replace('', None).astype(float)
    df['quantity_fact'] = df['quantity_fact'].replace('', None).astype(float)

    #формируем логичный порядок столбцов, удаляем ненужные столбцы и янвные дубли
    df = df[['loaded_ts',
             'task_name',
             'task_id',
             'task_creation_dt',
             'board_name',
             'column_name',
             'task_status',
             'subtask_id',
             'subtask_name',
             'subtask_status',
             'quantity_plan',
             'quantity_fact',
             'delivery_term',
             'loading_place',
             'loading_start_date',
             'loading_end_date',
             'ship_name',
             'discharging_place',
             'deadline_start_date',
             'deadline_end_date',
             'prov_paid',
             'final_paid']].drop_duplicates(ignore_index=True)

    #пуш xcom полученного датафрейма в следующую задачу по инсерту в бд insert_result_df_to_db()
    ti.xcom_push(key='df_result', value=df)

#функция записи датафрейма в базу данных
def insert_result_df_to_db(table, ti):

    #берем переданный из prepare_result_df_for_insert() итоговый датафрейм через xcom
    df = ti.xcom_pull(key='df_result')
    #заменим все незаполненные значения на None - необходимо для корректного инсерта в БД
    df = df.replace({np.nan: None})
    #загрузим данные
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.insert_rows(table, df.values.tolist(), target_fields=df.columns.tolist())

#описание DAG
with DAG('tradingops-yougile-loading-cdm-tasks3',
          default_args=args,
          description='get from yg and load to db data about taks',
          catchup=True,
          start_date=datetime(2024,6,21,4,0,0),
          schedule_interval =  '0 5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 * * *', #пайплайн выполняется каждый час в течение дня (ночью по мск не выполняется)
          on_failure_callback=on_failure_callback) as dag:

    #старт
    begin = EmptyOperator(task_id='begin')

    #получение данных о досках и колонках в них из YouGile
    get_df_yg_brd_clmn = PythonOperator(task_id='get_df_yg_brd_clmn',
                                        python_callable=get_df_yg_brd_clmn)

    #получение данных о текстовых стикерах из YouGile
    get_df_yg_str_stickers = PythonOperator(task_id='get_df_yg_str_stickers',
                                            python_callable=get_df_yg_str_stickers)
    
    #получение данных о задачах и их подзадачах из YouGile
    get_df_yg_tasks = PythonOperator(task_id='get_df_yg_tasks',
                                     python_callable=get_df_yg_tasks)
    
    #подготовка результирующего датафрейма для инсерта в БД
    prepare_result_df_for_insert = PythonOperator(task_id='prepare_result_df_for_insert',
                                                  python_callable=prepare_result_df_for_insert,
                                                  op_kwargs={'execution_date': '{{data_interval_end}}'})

    #грузим результирующий датафрейм в БД
    insert_result_df_to_db = PythonOperator(task_id='insert_result_df_to_db',
                                            python_callable=insert_result_df_to_db,
                                            op_kwargs={'table': 'tops_yg.cdm_tasks'})

    #финиш
    end = EmptyOperator(task_id='end')

    begin >> (get_df_yg_brd_clmn, get_df_yg_str_stickers) >> get_df_yg_tasks >> prepare_result_df_for_insert >> insert_result_df_to_db >> end
