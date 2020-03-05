# wsbdwaterbalansapp

Voorbeeld van simpel waterbalans dashboard voor geclusterde peilgebieden (combinatie van meerdere peilgebieden). Maakt gebruik van meerdere bronnen oa.
- KIWIS-API (te vervangen door bijv. FEWS-API, of CSV-bestanden) voor debietreeksen; 
- HydroNET-FTP (te vervangen door bijv. KNMI-API, zie hieronder een voorbeeldje) voor neerslag en verdamping.

```python
from io import StringIO
import pandas as pd
import requests

url_dag = 'http://projects.knmi.nl/klimatologie/daggegevens/getdata_dag.cgi'
params = {'start': '20180101', 'vars': ['PRCP', 'EV24'], 'stns': '350'} # neerslag en verdamping van station Gilze Rijen (350) vanaf 1 januari 2018 tot nu
r = requests.get(url_dag, params=params)
columnnames = [column.strip() for column in StringIO(r.text).read().split("#")[-2].split(',')] # split op comment #, pak de
# een-na-laatste dit zijn de kolomnamen, split weer met komma, en strip() de kolomnamen van spaties en /r/n
p = pd.read_csv(StringIO(r.text), comment='#', sep=',', names=columnnames, parse_dates=[1], index_col=[1])
```

PS.
Qua data mist er een shapefile (Geclusterde_peilvakken.shp), ik kreeg deze zo snel niet geupload.

![dashboard](https://github.com/thomas-wsbd/wsbdwaterbalansapp/blob/master/img/dashboard.PNG)
