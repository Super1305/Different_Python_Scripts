#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[21]:


bikes_Q1=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q1_sample.csv', 
                     date_parser=['start_time', 'end_time'])


# In[22]:


bikes_Q1.head()


# In[23]:


bikes_Q1.start_time=pd.to_datetime(bikes_Q1.start_time)


# In[25]:


bikes_Q1.set_index('start_time', inplace = True)


# In[26]:


bikes_Q1


# В данных имеется как дата аренды, так и её точное время начала и окончания с точностью до секунд. Примените метод pd.resample() и агрегируйте данные по дням. В качестве ответа укажите максимальное число аренд за день.

# In[27]:


bikes_Q1.end_time=pd.to_datetime(bikes_Q1.end_time)


# In[33]:


bikes_Q1.resample(rule='D').count().max()


# In[34]:


bikes_Q1.resample(rule='D').


# In[ ]:





# Посмотрим на распределение количества аренд для разных групп пользователей (usertype) — customers и subscribers в данных за апрель. Данные за нужный период можно подгрузить отсюда.
# 
# Сделайте ресемпл по дням для каждой группы и в качестве ответа укажите число аренд за 18 апреля, сделанных пользователями типа Subscriber.
# 
# Может пригодиться:
# 
# функция для транспонирования .T

# In[115]:


usertype=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_april.csv')


# In[48]:


usertype


# In[116]:


usertype.start_time=pd.to_datetime(usertype.start_time)


# In[117]:


usertype.end_time=pd.to_datetime(usertype.end_time)


# In[53]:


usertype.info()


# In[137]:


def checker(date1, date2, z=datetime(2018,4,18)):
    if date1<=z<=date2:
        return 1
    else:
        return 0

# check('2018-04-01', '2018-04-20')


# In[138]:


x=usertype.shape[0]
x


# In[148]:


usertype.start_time


# In[139]:


i,j,m=0,0,[]
while i<x:
    if checker(usertype.start_time[i], usertype.end_time[i])==1:
        m.append(int(i))
        j+=1
    i+=1
        
print(j, m, i)


# In[132]:


for d in m:
    usertype.iloc[d]


# In[ ]:





# In[149]:


bikes=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_april.csv', parse_dates=[0,2])


# In[150]:


usertype.info()


# In[151]:


bikes.set_index('start_time', inplace=True)


# In[152]:


bikes.head()


# In[155]:


bikes.groupby('usertype')     .trip_id     .resample('1d')    .count()


# In[286]:


df1=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q2_sample_apr.csv', parse_dates=[1,2])
df2=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q2_sample_may.csv', parse_dates=[1,2])
df3=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q2_sample_jun.csv', parse_dates=[1,2])
df4=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q3_sample_july.csv', parse_dates=[1,2])
df5=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q3_sample_aug.csv', parse_dates=[1,2])
df6=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q3_sample_sep.csv', parse_dates=[1,2])
df7=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q4_sample_oct.csv', parse_dates=[1,2])
df8=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q4_sample_nov.csv', parse_dates=[1,2])
df9=pd.read_csv('https://stepik.org/media/attachments/lesson/367415/bikes_q4_sample_dec.csv', parse_dates=[1,2])
frames = [df1, df2, df3, df4, df5, df6, df7, df8, df9]
bikes = pd.concat(frames)





# Объедините сэмплы данных за нужные месяцы в один общий датасет bikes. Сделайте преобразование по дням для каждой группы пользователей (usertype), затем выберите дни, в которые число аренд, сделанных customers, было больше, чем у subscribers.

# In[ ]:





# In[ ]:





# In[200]:


bikes.set_index('start_time', inplace=True)


# In[201]:


rents_byday_bytype=bikes.groupby('usertype').resample('1d').size()


# In[202]:


rents_byday_bytype


# In[ ]:





# In[203]:


new=rents_byday_bytype.T


# In[204]:


new


# In[205]:


new[new.Customer>new.Subscriber]


# Еще один плюс использования дат в качестве индексов – возможность выбрать данные за интересующий нас промежуток времени. В переменную bikes_summer сохраните наблюдения с 1 июня по 31 августа. Затем запишите в top_destination наиболее популярный пункт назначения (его название). Агрегируйте данные по дням и определите, в какой день в полученный пункт (top_destination) было совершено меньше всего поездок. Дату сохраните в bad_day, отформатировав timestamp с помощью .strftime('%Y-%m-%d').
# 
# Могут пригодиться:
# 
# loc
# strftime
# idxmin, idxmax
# size
# query
# Полный датафрейм уже сохранен как bikes, еще раз объединять данные в этом степе не нужно :) В качестве индексов используется start_time в нужном формате.

# In[208]:


bikes.head()


# In[212]:


bikes_summer=bikes.query('"2018-06-01"<=start_time<="2018-08-31"')    # 1


# In[235]:


prem=bikes_summer.groupby('to_station_name', as_index=False)            .agg({'to_station_id':'count'})            .sort_values('to_station_id',ascending=False)            #.max()[0]
            


# In[242]:


prem.head()


# In[248]:


prem.reset_index(drop=True).to_station_name[0]


# In[254]:


top_destination=bikes_summer.groupby('to_station_name', as_index=False)            .agg({'to_station_id':'count'})            .sort_values('to_station_id',ascending=False)            .reset_index(drop=True).to_station_name[0]          #    2
top_destination


# In[263]:


bikes_summer.set_index('start_time', inplace=True)     #     3


# In[265]:


bikes_summer.head()


# In[274]:


bad_day=bikes_summer.query('to_station_name==@top_destination')                    .resample('1d')                    .count()                    .sort_values('to_station_id')                    .idxmin()[0]                    .strftime('%Y-%m-%d')       # 4


# In[275]:


bad_day


# Куда больше всего ездят на выходных? Туда же, куда и в будние дни, или в другие пункты назначения?
# Используя данные за период с 1 июня по 31 августа, выберите верные утверждения.
# df['weekday'] = df['datetime'].dt.day_name

# In[287]:


bikes_summer=bikes.query('"2018-06-01"<=start_time<="2018-08-31"')    # 1


# In[288]:


bikes_summer.head()


# In[293]:


# bikes_summer.loc[bikes_summer['start_time'].dt.day_name]
import datetime as dt
bikes_summer['weekday'] = bikes_summer[['start_time']].apply(lambda x: dt.datetime.strftime(x['start_time'], '%A'), axis=1)


# In[294]:


bikes_summer.head()


# In[310]:


bikes_summer.groupby(['weekday','to_station_name']).to_station_id.count().loc[['Monday','Tuesday','Wednesday','Thursday','Friday']].sort_values()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




