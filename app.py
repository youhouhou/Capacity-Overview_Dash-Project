# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import pyodbc
import pandas as pd
import datetime
from datetime import datetime as dt
from dash.dependencies import Input, Output, Event
import plotly.graph_objs as go


### globle functions ###

# load data into dataframe and clean it up
def build_df(rows):
    df = pd.DataFrame([[ij for ij in i] for i in rows])
    df.rename(columns = {0:'Date',1:'Cap_id',2:'Ratio'},inplace=True)
    df['Ratio'] = [0 if i is None else i for i in df['Ratio']]
    df.loc[:,'Cap_id'] = [int(i) for i in df['Cap_id']]
    df.loc[:,'Ratio'] = [round(float(i),2)*100 for i in df['Ratio']]
    df['Cap_id'] = df['Cap_id'].astype('int32')
    df['Ratio'] = df['Ratio'].astype('float32')
    return df

# param: input data framea, product capacity dic, date format should be datetime
def calculate_cap(df,p_type,start_date,end_date,site_number,df_holiday):
    if start_date>end_date:
        return [],[]
    start_date = dt.strptime(start_date,'%Y-%m-%d').date()
    end_date = dt.strptime(end_date,'%Y-%m-%d').date()
    capacities = []
    times = []
    while start_date <= end_date:
        cap_tmp_store = []
        start_date_count = start_date.timetuple().tm_yday
        start_date_year = start_date.year
        if df_holiday[(df_holiday['year'] == start_date_year) & \
            (df_holiday['day_number'] == start_date_count) & (df_holiday['site_number'] == site_number)].shape[0] == 0:
            for k,v in p_type.items():
                time = offset_on_workday(start_date_count,start_date_year,abs(v),df_holiday,site_number)
                cap = df.loc[(df['Date']==time)&(df['Cap_id']==k),'Ratio']
                if cap.size == 0:
                    cap = 0
                else:
                    cap = cap.iloc[0]
                cap_tmp_store.append(cap)
            if cap_tmp_store: # empty list's boolean value is false in Python
                capacities.append(max(cap_tmp_store))
                times.append(start_date)
        start_date += datetime.timedelta(days = 1)
    return times , capacities

# this function will offset day based on current day, ignore all the dayoff
# @param:  date_count:number of days in year for current date, year: year of current date
# offset:offset based on current date for different cpacity calculation, df_holiday:store all the dayoff
def offset_on_workday(date_count,year,offset,df_holiday,site_number):
    if offset == 0:
        date = dt(year,1,1)+datetime.timedelta(days = date_count-1)
        return date
    elif date_count == 1 and offset > 0:
        new_date_count = dt(year-1,12,31).timetuple().tm_yday
        if df_holiday[(df_holiday['day_number'] == new_date_count) & (df_holiday['site_number'] == site_number)& (df_holiday['year']==year-1)].shape[0]>0:
            return offset_on_workday(new_date_count,year-1,offset,df_holiday,site_number)
        else:
            return offset_on_workday(new_date_count,year-1,offset-1,df_holiday,site_number)
    else:
        if df_holiday[(df_holiday['day_number'] == date_count-1) & (df_holiday['site_number'] == site_number)& (df_holiday['year']==year)].shape[0]>0:
            return offset_on_workday(date_count-1,year,offset,df_holiday,site_number)
        else:
            return offset_on_workday(date_count-1,year,offset-1,df_holiday,site_number)

# offset certain period based on current date
def date_offset_back_forward(current_date, offset_day):
    date_previous = current_date-datetime.timedelta(days = offset_day)
    date_future = current_date+datetime.timedelta(days = offset_day)
    return date_previous,date_future

# make date only show year/month/day
def truncate_date(date):
    return  datetime.date(date.year,date.month,date.day)

# find common product to compare:
def find_common_product(Cap_master,compare_list):
    total_list = []
    for i in compare_list:
        products_i = [k for k,v in Cap_master[i].items()]
        total_list += products_i
    count_dic = {i:total_list.count(i) for i in total_list}
    commmon_product = [k for k,v in count_dic.items() if v==len(compare_list) ]
    return commmon_product

# need functin to loop through Cap_master to grab all unique manufacturing process

# function for graph: generate data for cap Overview bar plot
def generate_cap_overview(dff,dff_holiday,start_date,end_date,data,holiday,factory):
    graph_data = []
    for k,v in Cap_master[factory].items():
        product_type = {}
        times , capacities = calculate_cap(dff,v,start_date,end_date,site_number[factory],dff_holiday)
        product_type['x'] = times
        product_type['y'] = capacities
        product_type['type'] = 'bar'
        product_type['name'] = k
        product_type['marker'] = {'font':{'size':20}}
        graph_data.append(product_type)
    return {
            'data': graph_data,
            'layout': {
                    'xaxis':{'title':'Time','titlefont':{'size':22},'tickfont':{'size':20}},
                    'yaxis':{'title':'Capacity(%)','titlefont':{'size':22},'tickfont':{'size':20}},
                    'title': '',
                    'height':700,
                    'plot_bgcolor': colors['background'],
                    'paper_bgcolor': colors['background'],
                    'legend':{'font':{'size':20}}
                }
            }







### define globle variable ###
date_now = dt.now()
this_year = date_now.year
# offset 180 days based on current date
date_previous, date_future = date_offset_back_forward(date_now,180)
previous_year = date_previous.year
previous_day_count = date_previous.timetuple().tm_yday
future_year = date_future.year
future_day_count = date_future.timetuple().tm_yday


site_number = {'Duarte':2,'MVC':3,'EuroTek':7,'Sonax':4}
Cap_master = {'Duarte':{
             'Window':{220:-2,225:-1},'CM':{226:-1},'SLD':{221:-2,228:-1},
             'SWD':{228:-1},'Arch(NT)':{240:-4},'Arch(WT)':{240:-6}
             },
             'MVC':{
              'Window':{320:-3,325:-2},'CM':{326:-2},'SLD':{321:-3,328:-2},
              'SWD':{328:-2},'Arch(NT)':{340:-5},'Arch(WT)':{340:-7}
             },
             'EuroTek':{},
             'Sonax':{
                 'Window':{420:-3},'CM':{421:-3},'SLD & MTD':{422:-3},
                 'SWD & BFD':{423:-3}
             }}


### connect to database ###
conn = pyodbc.connect(
    r'DRIVER={ODBC Driver 11 for SQL Server};'
    r'SERVER=192.168.1.16;'
    r'DATABASE=CANTOR;'
    r'UID=valuerd;'
    r'PWD=Value1818;'
    )


query = """select KB.datum, KB.kapaid, sum(kapawert) / nullif(SOLLKAPA,0)
        from KAPABUCH kb
        left join kapasoll ks on ks.datum = kb.datum and ks.kapaid = kb.kapaid
        where KB.kapaid in (220,225,226,221,228,240,320,325,326,321,328,340,420,421,422,423)
        group by KB.datum, KB.kapaid, SOLLKAPA
        order by KB.datum, KB.kapaid, SOLLKAPA"""

query_holidy = """
                select *
                from MANDKAL
                where (JAHR = ? and LFDNRTAG >= ?) or (JAHR = ? and LFDNRTAG<= ?)"""


cur = conn.cursor()
cur.execute(query)
rows = cur.fetchall()
df = build_df(rows)

cur_t = conn.cursor()
cur_t.execute(query_holidy,previous_year,previous_day_count,future_year,future_day_count)
rows = cur_t.fetchall()
df_holiday = pd.DataFrame([[ij for ij in i] for i in rows])
df_holiday.rename(columns = {0:'year',1:'day_number',2:'site_number'},inplace=True)

### CSS ###

colors = {
    'background': '',
    'text': ''
}

### create plotly layout ###
app = dash.Dash(__name__)
vertical = True
app.scripts.config.serve_locally = True
app.layout = html.Div(style = {'backgroundColor': colors['background']},
    children=[
    html.H1(children='Factory Capacity Overview',
            style={
            'textAlign': 'center',
            'font-size': '2.65em'}),

    html.Div(children='''
        Show Cpacity By Bar Chart.
        ''',
        style={
        'textAlign': 'center',
        'font-size': '1.5em'}),

# tab on the left
    html.Div(
        dcc.Tabs(
            tabs=[
                {'label': 'Capacity Overview', 'value': 1},
                {'label': 'Capacity Comparation', 'value': 2}
            ],
            value=1,
            id='tabs',
            vertical=vertical,
            style={
                'height': '100vh',
                'borderRight': 'thin lightgrey solid',
                'textAlign': 'left'
            }
        ),
        style={'width': '10%', 'float': 'left'}
    ),
# main display on the right
    html.Div( id = 'main_display',
        children = [
            # component for cap Overview bar plot
            html.Div( id = 'components_overview',
                children = [
                    html.Div([
                            html.Div(
                             style = {'width': '65%', 'display': 'inline-block'}
                            ),
                            html.Div(
                                dcc.DatePickerRange(
                                    id='date-picker-range',
                                    min_date_allowed = dt(2015, 1, 1),
                                    start_date = datetime.date(dt.now().year,dt.now().month,dt.now().day)+datetime.timedelta(days = 3),
                                    end_date = datetime.date(dt.now().year,dt.now().month,dt.now().day)+datetime.timedelta(days = 12),
                                    calendar_orientation='vertical'
                                )
                            , style={'width': '30%', 'float': 'right', 'display': 'inline-block'} )

                        ]),

                    html.Div(
                        [html.Div(
                            style = {'width': '65%', 'display': 'inline-block'}
                        ),
                        html.Div([
                            dcc.RadioItems(
                            id = 'factory',
                            options=[
                                {'label': 'Duarte', 'value': 'Duarte'},
                                {'label': 'MVC', 'value': 'MVC'},
                                {'label': 'Sonax', 'value': 'Sonax'}
                                ],
                            value='Duarte',style = {'font-size':'1em'})

                        ], style = {'width': '30%', 'float': 'right', 'display': 'inline-block'})],
                        style = {'padding': '20px 0px 30px 0px'}
                    )
                ], style = {'display': 'block'}),

            html.Div(id = 'components_comparation',
                    children = [
                                html.Div(
                                        [html.Div(
                                            style = {'width': '65%', 'display': 'inline-block'}
                                        ),
                                        html.Div(
                                            dcc.Dropdown(
                                                options=[
                                                    {'label': 'Duarte', 'value': 'Duarte'},
                                                    {'label': 'MVC', 'value': 'MVC'}
                                                ],
                                                value=['Duarte', 'MVC'],
                                                multi=True
                                            ), style = {'width': '30%', 'float': 'right', 'display': 'inline-block'}
                                        )],style = {'padding': '20px 0px 30px 0px'}
                                )
                            ],style = {'display': 'none'}
            ),

            # html.Div([
            #         dcc.DatePickerRange(
            #         id='date-picker-range',
            #         min_date_allowed = dt(2015, 1, 1),
            #         start_date = datetime.date(dt.now().year,dt.now().month,dt.now().day)+datetime.timedelta(days = 3),
            #         end_date = datetime.date(dt.now().year,dt.now().month,dt.now().day)+datetime.timedelta(days = 12),
            #         calendar_orientation='vertical'
            #         )
            #     ]),
            #
            # html.Div([
            #     dcc.RadioItems(
            #     id = 'factory',
            #     options=[
            #         {'label': 'Duarte', 'value': 'Duarte'},
            #         {'label': 'MVC', 'value': 'MVC'},
            #         {'label': 'Sonax', 'value': 'Sonax'}
            #         ],
            #     value='Duarte')
            #
            # ], style = {'padding': '20px 0px 0px 0px'}),
            dcc.Graph(id='Cpacity_Bar')
        ],
        style={'width': '90%', 'float': 'right'}),

    dcc.Interval(
        id='interval-component',
        interval=60*1000 # in milliseconds
        ),

    html.Div(id='cap_result', style={'display': 'none'}, children = df.to_json()),

    html.Div(id='holiday_result', style={'display': 'none'}, children = df_holiday.to_json())

])

### callback functions ###
# update the date from database
@app.callback(Output('cap_result', 'children'),
    events=[Event('interval-component', 'interval')]
    )
def update_data():
    cur_c = conn.cursor()
    cur_c.execute(query)
    rows = cur_c.fetchall()
    dff = build_df(rows)
    return dff.to_json()

# update graph
@app.callback(
    Output('Cpacity_Bar', 'figure'),
    [Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date'),
     Input('cap_result','children'),
     Input('holiday_result','children'),
     Input('factory','value'),
     Input('tabs','value')]
     )
def update_graph(start_date,end_date,data,holiday,factory,tab_value):
    dff = pd.read_json(data)
    dff_holiday = pd.read_json(holiday)
    if tab_value == 1:
        return generate_cap_overview(dff,dff_holiday,start_date,end_date,data,holiday,factory)
    elif tab_value == 2:
        return {}

@app.callback(
    Output('components_overview','style'),
    [Input('tabs','value')]
    )
def diplay_component_overview(value):
    if value == 1:
        return {'display': 'block'}
    elif value == 2:
        return {'display': 'none'}

@app.callback(
    Output('components_comparation','style'),
    [Input('tabs','value')]
    )
def diplay_component_overview(value):
    if value == 2:
        return {'display': 'block'}
    elif value == 1:
        return {'display': 'none'}

if __name__ == '__main__':
    app.run_server(debug=True)
