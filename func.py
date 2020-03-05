import requests
import pandas as pd
import geopandas as gpd
import os

os.chdir('G:\\WS_KenA\\Per_persoon\\Thomas\\data\\python\\dashboards\\productie\\vpwks565')

def create_meta():
    """create geodataframe containing metadata based on shapefile and metadata.csv file"""
    gdf = gpd.read_file('data/shp/Geclusterde_peilvakken.shp')
    gdf.set_index('Naam_Clust', inplace=True)
    m = pd.read_csv('data/meta/meta.csv')
    gdf['peilvakken'] = ''
    gdf['gemalen'] = ''
    gdf['inlaten'] = ''
    gdf['I'] = ''
    gdf['G'] = ''
    gdf['A'] = gdf.geometry.area/(100*100) # oppervlakte in ha toevoegen

    for i in m.Cluster.unique(): # voor alle gebieden de metadata toevoegen
        gdf.at[i, 'peilvakken'] = list((m[m.Cluster==i].Peilvakken.dropna()))
        gdf.at[i, 'inlaten'] = list(m[m.Cluster==i].Inlaten.dropna())
        gdf.at[i, 'I'] = list(m[m.Cluster==i].I.dropna())
        gdf.at[i, 'gemalen'] = list(m[m.Cluster==i].Gemalen.dropna())
        gdf.at[i, 'G'] = list(m[m.Cluster==i].G.dropna())
    return gdf

def get_meetreeks(station_no):
    """meetgegevens binnenhalen voor een stations nummer uit de metadata van de KIWIS-API"""
    url_ts_id =  ('http://10.10.3.126:8080/KiWIS/KiWIS?service=kisters&type=queryServices&request=getTimeseriesList'
                  '&datasource=0&format=objson&ts_name=Dag.Gem&station_no=%s' % station_no)
    r = requests.get(url_ts_id)
    ts_id = r.json()[0]['ts_id'] # timeseries id uit response halen (ts_id)
    
    url_series = ('http://10.10.3.126:8080/KiWIS/KiWIS?service=kisters&type=queryServices&request=getTimeseriesValues'
                  '&datasource=0&format=json&from=2016-1-1&metadata=True&dateformat=yyyy-MM-dd&ts_id=%s'% ts_id)
    r = requests.get(url_series) # met ts_id reeks ophalen, vananf 2016 tot nu
    df = pd.DataFrame(r.json()[0]['data'], columns=['date', station_no]) # dataframe van gegevens maken
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df

def get_data(Cluster):
    """alle data ophalen voor een cluster"""
    # metadata
    gdf = create_meta()
    
    # niet beschikbare reeksen opslaan in een list
    x = []
    
    # tijdreeksen
    df = pd.DataFrame()
    
    # neerslag
    P = pd.read_csv('data/reeksen/neerslag.csv', parse_dates=['Eind'], index_col=['Eind'], usecols=['Eind', Cluster])
    df['P'] = P[Cluster] # neerslag toevoegen aan DataFrame
    
    # verdamping
    EV = pd.read_csv('data/reeksen/verdamping.csv', parse_dates=['Eind'], index_col=['Eind'], usecols=['Eind', Cluster])
    df['Ea'] = EV.resample('d').mean() * -1 # verdamping toevoegen aan DataFrame, uit dus negatief
    
    # kwel
    K = pd.read_csv('data/reeksen/kwel.csv', index_col=['Cluster'])
    df['K'] = K.loc[Cluster][0]
    
    # wiski reeksen
    t1 = pd.DataFrame() # uit reeksen
    t2 = pd.DataFrame() # in reeksen
    
    try:
        for i in gdf.loc[Cluster,'G']: # gemaal meetreeksen uit WISKI halen
            try:
                t = get_meetreeks(i) * -1 # uit dus negatief
                t1 = pd.concat([t1, t], axis=1) # alle gemalen samenvoegen tot een DataFrame
                t1 = t1.sum(axis=1) # gemalen sommeren tot een waarde
                t1 = pd.DataFrame(t1)
                t1.columns = ['G']
            except:
                x.append(i)
                print('niet beschikbaar:', i)
        df = pd.concat([df, t1], axis=1)
    except:
        print('geen wiski data gemalen')
    
    try:
        for i in gdf.loc[Cluster,'I']: # inlaat meetreeksen uit WISKI halen
            try:
                t = get_meetreeks(i)
                t2 = pd.concat([t2, t], axis=1) # samenvoegen tot een DataFrame
                t2 = t2.sum(axis=1)
                t2 = pd.DataFrame(t2)
                t2.columns = ['I']
            except:
                x.append(i)
                print('niet beschikbaar:', i)
        df = pd.concat([df, t2], axis=1)
    except:
        print('geen wiski data inlaten')
    
    # omzetten van m3/s naar mm
    try:
        df['G'] = df['G'] * (60*60*24*1000) / (gdf.loc[Cluster,'A']*100*100) # van m3/s naar mm/dag
        df['I'] = df['I'] * (60*60*24*1000) / (gdf.loc[Cluster,'A']*100*100)
    except:
        pass
    
    # berging berekenen
        df['dS'] = df.sum(axis=1)
    
    return df['2016':], gdf, x

def update_from_sftp():
    """update de data (neerslag en verdamping) vanaf sftp server van hydronet"""
    import pysftp
    
    # neerslag update van sftp
    with pysftp.Connection('ftp.hydronet.nl', username='wsbd', password='******') as sftp:
        try:
            sftp.cwd('106/CSV/')
            sftp.get(sftp.listdir()[-1], 'data/reeksen/neerslagftp.csv') # laatste bestand binnenhalen en opslaan
            print('downloaded latest hydronet radar')
        except:
            print('no connection')
            
    # verdamping update van sftp        
    with pysftp.Connection('ftp.hydronet.nl', username='wsbd', password='******') as sftp:
        try:
            sftp.cwd('273/CSV/')
            sftp.get(sftp.listdir()[-1], 'data/reeksen/verdampingftp.csv') # laatste bestand binnenhalen en opslaan
            print('downloaded latest eleaf evaporation')
        except:
            print('no connection')
            
    # toevoegen aan oude reeksen neerslag      
    P = pd.read_csv('data/reeksen/neerslag.csv', parse_dates=['Eind'], index_col=['Eind'])
    t = pd.read_csv('data/reeksen/neerslagftp.csv', skiprows=2, parse_dates=['Eind'], index_col=['Eind'], dayfirst=True)
    t = t[[column for column in t.columns if 'Kwaliteit van' not in column]]
    t = t.resample('d').mean()
    d = t.combine_first(P)
    d.to_csv('data/reeksen/neerslag.csv') # opslaan als nieuwe reeks
    
    # toevoegen aan oude reeksen verdamping
    EV = pd.read_csv('data/reeksen/verdamping.csv', parse_dates=['Eind'], index_col=['Eind'])
    t = pd.read_csv('data/reeksen/verdampingftp.csv', skiprows=2, parse_dates=['Eind'], index_col=['Eind'], dayfirst=True)
    t = t[[column for column in t.columns if 'Kwaliteit van' not in column]]
    t = t.resample('d').mean()
    d = t.combine_first(EV)
    d.to_csv('data/reeksen/verdamping.csv') # opslaan als nieuwe reeks
