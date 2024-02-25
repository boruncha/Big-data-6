import API
import pandas as pd

API = API.Retake(host='127.0.0.1', username='postgres', password='12345', database='postgres')

data = {'fullname': ['Михалева Нина Алексеевна', 'Луань Екатерина Евгеньевна', 'Зубенко Михаил Петрович'],
'diagnosis':['Смешанная гиперлипидемия', 'Гиперлипидемия неуточненная', 'Другие уточненные поражения сосудов мозга']}
df = pd.DataFrame(data)
API.create_table(df, 'patients')

# API.delete_from_table('patients', conditions="diagnosis = 'Другие уточненные поражения сосудов мозга'")

# result = API.read_sql('patients')
# print(result)
#
# API.truncate_table('patients')
#
# data = {'fullname': ['Иванова	Валерия	Андреевна', 'Дидин Сергей Иванович', 'Ван Станислав Станиславович'],
# 'diagnosis': ['Стенокардия неуточненная', 'Астма неуточненная', 'Язва желудка хроническая без кровотечения или прободения']}
# df = pd.DataFrame(data)
# API.insert_sql(df, 'patients')
#
# query = "UPDATE users SET diagnosis = 'Язва желудка острая без кровотечения или прободения' WHERE fullname = 'Ван Станислав Станиславович'"
# API.execute(query)
#
# result = API.read_sql('patients')
# print(result)
