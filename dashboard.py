import dash
import func

import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import geopandas as gpd

from dash.dependencies import Input, Output
from apscheduler.schedulers.background import BackgroundScheduler

# update neerslag en verdamping, dit komt van een ftp server van hydronet
scheduler = BackgroundScheduler()
scheduler.start()
scheduler.add_job(func.update_from_sftp, trigger='cron', hour='14')

# store, hierin wordt het laatste gebied opgeslagen
store = {'cluster': 'Bloemendaalse polder'}

# global, globale parameters die in alle functies worden gebruikt
d, gdf, x = func.get_data(store['cluster']) # data ophalen
r = d.resample('10d').mean() # data resamplen naar decaden
t = r['20190401':'20190930'] # laatste groeiseizoen selecteren
colors = ['#1f77b4', '#ff7f0e', '#2ca02c' , '#9467bd', '#d62728'] # verschillende kleuren voor verschillende posten

# app opbouw van dash-app
app = dash.Dash(__name__)
app.css.config.serve_locally = False
app.css.append_css({'external_url': 'https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css'})
app.title = 'Waterbalansen'

app.layout = html.Div([html.Div(
            dcc.Tabs(
                children=[dcc.Tab(label=i, value=i) for i in gdf.index], # tabbladen voor alle gebieden
                value='Bloemendaalse polder', # actieve gebied bij opstarten
                id='tabs',
                vertical=True,
                style={'height': '100vh',
                       'borderRight': 'ligthgrey solid',
                       'textAlign': 'left'}),
                style={'width':'15%', 'float':'left', 'padding': '20px 20px 20px 0px'}),
                html.H2('Waterbalansen geclusterde peilgebieden'),
            dcc.RadioItems(
                id='agg',
                options = [{'label': i, 'value': i} for i in ['d', '10d', 'm', 'y']], # verschillende aggregatie opties
                value='10d',
                labelStyle={'display': 'inline-block'}),
            dcc.RadioItems(
                id='jaar',
                options = [{'label': i, 'value': i} for i in ['2016', '2017', '2018', '2019', '2020']], # verschillende jaren
                value='2019',
                labelStyle={'display': 'inline-block'}),
            dcc.RadioItems(
                id='periode',
                options = [
                        {'label':'groeiseizoen', 'value':'g'},
                        {'label':'gehele jaar', 'value':'h'}, # groeiseizoen of complete jaar
                        ],
                value='g',
                labelStyle={'display': 'inline-block'}),
            dcc.RadioItems(
                id='eenheid',
                options = [{'label': i, 'value': i} for i in ['mm/d', 'mm', 'm3/s']], # eenheid
                value='mm/d',
                labelStyle={'display': 'inline-block'}),
            html.Hr(),
            html.Div(html.Div(id='tab-output'),
                style={'width': '80%', 'float': 'right'})])

@app.callback(Output('tab-output', 'children'), [Input('tabs', 'value'), Input('agg', 'value'), Input('jaar', 'value'),
                                                Input('periode', 'value'), Input('eenheid', 'value')]) # als een van de waarden word veranderd

def display_content(tab_value, agg_value, year_value, period_value, unit_value):
    
    global gdf, store, d, r, t, colors, x
    
    if not store['cluster'] == tab_value: # als gebied verandert nieuwe data binnenhalen
        d, gdf, x = func.get_data(tab_value)
        
    if period_value == 'g': # periode groeiseizoen
        r = d[year_value+'0401':year_value+'0930']
    else:
        r = d[year_value]
    t = r.resample(agg_value).mean()
          
    print(store)
    
    if unit_value == 'm3/s':
        t = t / (60*60*24*1000) * (gdf.loc[tab_value,'A']*100*100)
    if unit_value == 'mm':
        t = r.resample(agg_value).sum()
    
    a = []
    for idx, column in enumerate(t.columns): # list opbouwen met data voor plot
        if column == 'dS': # storage, berging heeft andere styling
            a.append({'x':    t.index,
                      'y':    t[column].values,
                      'name': column,
                      'mode': 'markers+lines',
                      'line': {'color': 'black', 'width': 2}})
        else: # andere termen toevoegen met juiste kleur
            a.append({'x':     t.index,
                      'y':     t[column].values,
                      'name':  column,
                      'type':  'bar',
                      'marker': {'color': colors[idx]}})
    return dcc.Graph(
            id='waterbalans',
            figure={
                'data': a,
                'layout': {
                        'title':   tab_value + ', ' + year_value + ' ' + ' [' + unit_value + '], <br> (nog) niet beschikbare reeksen: ' + ', '.join(x),
                        'xaxis':   {'type': 'date'},
                        'yaxis':   {'title': unit_value, 'hoverformat': '.2f'},
                        'barmode': 'relative',
                        'height':  800
                        } 
            }
        )


if __name__ == '__main__':
    app.run_server(port=80, host='vpwks565.wsbd.nl')
