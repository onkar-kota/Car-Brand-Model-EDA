import pandas as pd
import numpy as np
import timeit
# import files as f
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import boto3
import streamlit as st


s3 = boto3.resource(
    service_name = 's3',
    region_name = 'ap-south-1',
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY
)
obj = s3.Bucket('analyticss').Object('vehicle_data.csv').get()


data = pd.read_csv(obj['Body'])
# data = pd.read_csv('D:/Coding Files/Coding Ninja/5. Projects/4. Car Brand model EDA/vehicle_data.csv')
data['Price'] = data['Price'].str.replace('Rs','')
data['Price'] = data['Price'].apply(lambda x: float(str(x).replace(',','')))
data['Capacity'] = data['Capacity'].str.replace('cc','')
data['Capacity'] = data['Capacity'].apply(lambda x: float(str(x).replace(',','')))
data['Mileage'] = data['Mileage'].str.replace('km','')
data['Mileage'] = data['Mileage'].apply(lambda x: float(str(x).replace(',','')))
data = data.rename(columns={'Price':'Price_rs','Capacity':'Capacity_cc','Mileage':'Mileage_km'})

data = data.drop(['Sub_title','Edition'],axis=1)
data['Brand_Model'] = data['Brand'] + " " + data['Model']
data = data.drop(['Brand','Model'],axis=1)
data = data.rename(columns={'Brad_Model':'Brand_Model'})
data['Body'] = data['Body'].fillna(data['Body'].mode()[0])

car = data[['Brand_Model','Price_rs','Year','Condition','Transmission','Body','Fuel','Capacity_cc','Mileage_km','Seller_name','Seller_type']]
car['Serial'] = car.index
car = car[['Serial','Brand_Model','Price_rs','Year','Condition','Transmission','Body','Fuel','Capacity_cc','Mileage_km','Seller_name','Seller_type']]
car['Year'] = pd.to_datetime(car['Year'].astype(str)).values

New_cars  = len(car[car['Condition'] == 'New'])
Reconditioned_cars = len(car[car['Condition'] == 'Reconditioned'])
Used_cars = len(car[car['Condition'] == 'Used'])


def carPriceData(data):
    rating = data.groupby(['Price_rs','Brand_Model','Condition']).agg({'Serial':'count'}).reset_index()
    rating = rating[rating['Serial'] != 0]
    rating.columns = ['Price_rs','Brand_Model','Condition','count']
    rating = rating.sort_values('Price_rs',ascending=False)
    return rating
    
carPrice = carPriceData(car)

def carConditionData(data):
    rating = data.groupby(['Brand_Model', 'Condition','Transmission']).agg({'Serial': 'count'}).reset_index()
    rating = rating[rating['Serial'] != 0]
    rating.columns = ['Brand_Model', 'Condition','Transmission','count']
    rating = rating.sort_values('count',ascending=False)
    return rating

car_new = car[car['Condition'] == 'New']
car_re = car[car['Condition'] == 'Reconditioned']
car_use = car[car['Condition'] == 'Used']

car_newdf = carConditionData(car_new)
car_redf = carConditionData(car_re)
car_usedf = carConditionData(car_use)

data2 = car[['Price_rs','Seller_name']].groupby('Seller_name').sum()
data2 = pd.DataFrame(data2.to_records())
data2 = data2.sort_values(by=['Price_rs'], ascending=False)
data2 = data2[:10]

cond = data[['Brand_Model','Price_rs','Year','Condition','Mileage_km']]
def conditionPriceCompare(model):
    result = []
    
    #new
    data_new = cond[(cond['Brand_Model'] == model) & (car['Condition'] == 'New')]
    data_new = data_new.sort_values(by='Mileage_km',ascending=False)
    if len(data_new) > 0:
        result.append(data_new[:1].values[0])
    else:
        print('Car in this condition not available')
        result.append([model,0,0,'New',0])
    
    #reconditioned
    data_recond = cond[(cond['Brand_Model'] == model) & (car['Condition'] == 'Reconditioned')]
    data_recond = data_recond.sort_values(by='Mileage_km',ascending=False)
    if len(data_recond) > 0:
        result.append(data_recond[:1].values[0])
    else:
        print('Car in this condition not available')
        result.append([model,0,0,'Reconditioned',0])
        
    #used
    data_used = cond[(cond['Brand_Model'] == model) & (car['Condition'] == 'Used')]
    data_used = data_used.sort_values(by='Mileage_km',ascending=False)
    if len(data_used) > 0:
        result.append(data_used[:1].values[0])
    else:
        print('Car in this condition not available')
        result.append([model,0,0,'Used',0])
        
    return result

res = conditionPriceCompare('Nissan Dayz')

df_cond = pd.DataFrame(res,columns=['Brand_Model','Price_rs','Year','Condition','Mileage_km'])

new = car[car['Condition'] == 'New']
new_2 = new[['Condition','Seller_name']].groupby('Seller_name').count()
new_2 = pd.DataFrame(new_2.to_records())
new_car_seller = new_2.sort_values(by='Condition',ascending=False).values[:5]
newdf = pd.DataFrame(new_car_seller, columns=['Seller_name','Count'])
newdf['Condition'] = 'New'

old = car[car['Condition'] == 'Used']
old_2 = old[['Condition','Seller_name']].groupby('Seller_name').count()
old_2 = pd.DataFrame(old_2.to_records())
old_cars_seller = old_2.sort_values(by='Condition',ascending=False).values[:5]
olddf = pd.DataFrame(old_cars_seller,columns = ['Seller_name','Count'])
olddf['Condition'] = 'Used'

recon = car[car['Condition'] == 'Reconditioned']
recon = recon[['Condition','Seller_name']].groupby('Seller_name').count()
recon = pd.DataFrame(recon.to_records())
recon_cars_seller = recon.sort_values(by='Condition',ascending=False).values[:5]
recondf = pd.DataFrame(recon_cars_seller,columns = ['Seller_name','Count'])
recondf['Condition'] = 'Reconditioned'

seller_con = pd.concat([newdf,olddf,recondf]).reset_index(drop=True)
seller_con['Count'] = seller_con['Count'].astype(int)

tran = data[['Brand_Model','Condition','Transmission','Body','Mileage_km','Price_rs']]
def TranModel(trantype, bodytype):
    data_new = tran[(tran['Transmission'] == trantype) & (tran['Body'] == bodytype)]
    data_new = data_new.sort_values(by='Mileage_km',ascending=False)
    return data_new
t = TranModel('Automatic','Hatchback')







# ------------------ Dashboard ------------------ #

st.title('Car Brand Model :red[EDA] :bar_chart:')

tab1, tab2 = st.tabs(['Data :gear:','Analysis :green_book:'])

with tab1:
    st.header('Car Dataset')
    st.write(data)
    
with tab2:
    # ------------------ Count of Used, New and Reconditioned Cars ------------------ #
    st.subheader('1. Count of Used, New and Reconditioned Cars')
    st.write("New Cars :", New_cars)
    st.write("Reconditioned Cars :", Reconditioned_cars)
    st.write("Used Cars :", Used_cars)
    
    # ------------------ Price of Cars Brand Model------------------ #
    st.subheader('2. Price Of Cars Brand Model')
    fig = px.scatter(car, x=car['Brand_Model'], y=car['Price_rs'], size='Price_rs', color='Condition')
    st.plotly_chart(fig)
    
    # ------------------ Price of Cars ------------------ #
    st.subheader('3. Price of Cars Brand Model')
    fig = px.bar(carPrice[:20], x='Brand_Model',y='Price_rs', color='Condition')
    st.plotly_chart(fig)
    
    # ------------------ Transmission of Each Condition ------------------ #
    st.subheader('4. Transmission in Each Condition')
    fig = make_subplots(rows=1, cols=3, specs=[[{"type": "pie"}, {"type": "pie"}, {"type": "pie"}]])

    fig.add_trace(
        go.Pie(labels=car_newdf['Transmission'], values=car_newdf['count']),
        row=1, col=1
    )

    fig.add_trace(
        go.Pie(labels=car_redf['Transmission'], values=car_redf['count']),
        row=1, col=2
    )

    fig.add_trace(
        go.Pie(labels=car_usedf['Transmission'], values=car_usedf['count']),
        row=1, col=3
    )

    fig.update_traces(textposition='outside', hole=.4, hoverinfo="label+percent")

    fig.update_layout(
        title_text = "Transmission in Each Condition",
        annotations = [dict(text='New', x=0.12,y=0.5,font_size=12, showarrow=False),
                    dict(text='Reconditioned', x=0.50,y=0.5,font_size=12, showarrow=False),
                    dict(text='Used', x=0.88,y=0.5,font_size=12, showarrow=False)]
    )
    st.plotly_chart(fig)
    
    # ------------------ Best Sellers ------------------ #
    
    st.subheader('5. Best Sellers')
    fig = px.bar(data2,x=data2['Seller_name'],y=data2['Price_rs'],color='Price_rs')
    st.plotly_chart(fig)
    
    # ------------------ Condition Price Compare ------------------ #
    
    st.subheader('6. Condition Price Compare')
    fig = px.bar(df_cond,x=df_cond['Mileage_km'], y=df_cond['Price_rs'],color='Condition')
    st.plotly_chart(fig)
    
    # ------------------ Whcih seller has the most number of cars ------------------ #
    
    st.subheader('7. Whcih seller has the most number of cars?')
    fig = px.bar(seller_con,x=seller_con['Seller_name'],y=seller_con['Count'],color='Condition')
    st.plotly_chart(fig)
    
    # ------------------ Transmission vs condition ------------------ #
    
    st.subheader('8. Transmission vs condition')
    fig = px.histogram(tran, x=tran['Transmission'],color='Condition')
    st.plotly_chart(fig)  
    
    # ------------------ Body vs Condition ------------------ #
    
    st.subheader('9. Body vs Condition')
    fig = px.histogram(tran, x=tran['Body'],color='Condition')
    st.plotly_chart(fig)