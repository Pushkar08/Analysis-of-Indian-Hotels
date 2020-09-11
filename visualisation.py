import plotly.graph_objects as go
import plotly.express
import plotly.offline as pyo
import plotly as py
import plotly
import sqlalchemy
import psycopg2
from sqlalchemy import create_engine
import pandas.io.sql as sqlio
import matplotlib.pyplot as plt
import seaborn as sns


def aggregationAndVisualization():
    query1="""select property_name as Hotel, room_price as Price_per_Night from Stayzilla
            order by room_price desc limit 10;"""
    query2 ="""select property_name, image_count from stayzilla order by image_count desc limit 10"""    
    
    query3="""select count(unique_id) as Total, city from public.booking
              group by city order by Total desc limit 10"""
    
    query4 = """select count(unique_id) as Total, property_type from public.cleartrip
           group by property_type 
           order by Total desc"""
    
    query5="""select state, count(unique_id) as count from public.cleartrip group by state order by count desc limit 10;"""
    
    query6 ="""select count(unique_id) as Total, hotel_star_rating from public.booking
               group by hotel_star_rating order by Total;"""
                
    query7="""select * from \"Goibibo\""""
    
    query8="""select s.site_name, count(s.unique_id) as number_of_properties from stayzilla s group by s.site_name
            union all
            select b.site_name, count(b.unique_id) as number_of_properties from booking b group by b.site_name
            union all
            select c.site_name, count(c.unique_id) as number_of_properties from cleartrip c group by c.site_name
            union all
            select g.site_name, count(g.uniq_id) as number_of_properties from public."Goibibo" g group by g.site_name"""

    
    
    try:
        dbConnection = psycopg2.connect(
            user="postgres",  # dap
            password="2574929",  # dap
            host="localhost",  # 192.168.56.30
            port="5432",
            database="hotels")
        hotels_dataframe1 = sqlio.read_sql_query(query1, dbConnection)
        hotels_dataframe2 = sqlio.read_sql_query(query2, dbConnection)
        hotels_dataframe3 = sqlio.read_sql_query(query3, dbConnection)
        hotels_dataframe4 = sqlio.read_sql_query(query4, dbConnection)
        hotels_dataframe5 = sqlio.read_sql_query(query5, dbConnection)
        hotels_dataframe6 = sqlio.read_sql_query(query6, dbConnection)
        hotels_dataframe7 = sqlio.read_sql_query(query7, dbConnection)
        hotels_dataframe8 = sqlio.read_sql_query(query8, dbConnection)

    
    except(Exception, psycopg2.Error) as dbError:
        print("Error:", dbError)
    finally:
        if (dbConnection): dbConnection.close()
        
    visualization(hotels_dataframe1, hotels_dataframe2, hotels_dataframe3 ,hotels_dataframe4, hotels_dataframe5, hotels_dataframe6, hotels_dataframe7, hotels_dataframe8)
        
        
def visualization(hotels_dataframe1, hotels_dataframe2, hotels_dataframe3 ,hotels_dataframe4, hotels_dataframe5, hotels_dataframe6, hotels_dataframe7, hotels_dataframe8):
    
    data = plotly.express.data.gapminder()
    fig = plotly.express.bar(hotels_dataframe1, x='hotel', y='price_per_night',
             color='price_per_night',
             color_continuous_scale=plotly.express.colors.sequential.Cividis,
             labels={'hotel':'Hotel', 'price_per_night' : 'Price per Night'}, 
             title ='Top 10 Expensive Hotels - Stayzilla', height=600)
    fig.show(renderer = 'browser')
    
    
    
    size = hotels_dataframe2['image_count'] 
    fig = go.Figure(data=[go.Scatter(
    x=hotels_dataframe2['property_name'],
    y=hotels_dataframe2['image_count'],
    mode='markers',
    marker=dict(
        color=hotels_dataframe2['image_count'],#'aqua',
        size=size,
        sizemode='area',
        colorscale='Viridis',
        sizeref=2.*max(size)/(60.**2),     
        )
    )]) 
    fig.update_layout(title='Top 10 Properties Based on Image Count - Stayzilla',
                   xaxis_title='Image Count',
                   yaxis_title='Property Name') 
    fig.show(renderer = 'browser')
    
    
    
    plotly.offline.init_notebook_mode(connected=True)
    trace1 = go.Bar(
    x=hotels_dataframe3.city,
    y=hotels_dataframe3.total,
    name = 'Distribution of Hotels in Top 10 Cities - Booking.com')

    data = [trace1]
    layout = go.Layout(
    barmode='stack',
    margin=dict(b=100),
    title = 'Distribution of Hotels in Top 10 Cities - Booking.com')
    fig = go.Figure(data=data, layout=layout)
    fig.update_layout(xaxis_title = 'Cities', yaxis_title = 'Number of Hotels')
    #pyo.iplot(fig, filename='grouped-bar', renderer = 'browser')
    fig.show(renderer = 'browser')
    
    
    fig = go.Figure(data=go.Scatter(x=hotels_dataframe4['property_type'], y=hotels_dataframe4['total'],marker = dict(color = 'rgb(255, 0, 0)')))
    fig.update_layout(title_text='Hotels Distribution by Property Type - Cleartrip.com', xaxis_title = 'Property type', yaxis_title = 'Number of properties')
    fig.show(renderer = 'browser')
    
    
    py.offline.init_notebook_mode(connected=True)
    colors = ['Oxygen','Hydrogen','Carbon_Dioxide','Nitrogen']
    fig = go.Figure(data=[go.Pie(labels=hotels_dataframe5['state'] , values=hotels_dataframe5['count'])])
    fig.update_traces(hoverinfo='label+percent',
    textinfo='value', textfont_size=20,
    marker=dict(colors=colors,line=dict(color='#000000', width=2)), hole = 0.3)
    fig.update_layout(title_text='Number of Hotels per State - Cleartrip.com')
    fig.show(renderer = 'browser')
    
    #BAR CHART
    import plotly.express as px
    data = px.data.gapminder()
    fig = px.bar(hotels_dataframe6, x='hotel_star_rating', y='total',color='total',labels={'hotel_star_rating': 'Hotel Star Rating', 'total': 'Number of Hotels'}, height=600,title = "Hotel Distribution By Star Rating - Booking.com")
    fig.show(renderer = 'browser')


    sns.set_style("white")
    f, axarr = plt.subplots(2, 2, figsize=(14, 8))
    plt.suptitle('Goibibo Hotel Breakdown', fontsize=18)
    sns.kdeplot(hotels_dataframe7['site_review_rating'], ax=axarr[0][0])
    sns.kdeplot(hotels_dataframe7['site_review_count'], ax=axarr[0][1])
    sns.countplot(hotels_dataframe7['hotel_star_rating'], ax=axarr[1][0])
    sns.kdeplot(hotels_dataframe7['room_count'], ax=axarr[1][1])
    sns.despine()
    sns.jointplot(hotels_dataframe7.hotel_star_rating, hotels_dataframe7.site_review_rating)
    plt.show()
                

    py.offline.init_notebook_mode(connected=True)
    colors = ['Oxygen','Hydrogen','Carbon_Dioxide','Nitrogen']
    fig = go.Figure(data=[go.Pie(labels=hotels_dataframe8['site_name'] , values=hotels_dataframe8['number_of_properties'])])
    fig.update_traces(hoverinfo='label+percent',
    textinfo='value', textfont_size=20,
    marker=dict(colors=colors,line=dict(color='#000000', width=2)))
    fig.update_layout(title_text='Number of Properties per Site')
    fig.show(renderer = 'browser')