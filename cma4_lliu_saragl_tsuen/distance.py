import googlemaps
from datetime import datetime
import responses

gmaps = googlemaps.Client(key='AIzaSyD_SNKUiwDVf_LjMGyIZxLf9MMaWB2IqH0')

now = datetime.now()
origin = 41.43206,-81.38992 #latitude and longitude, ensure that there is no space between numbers & comma

class distance(dml.Algorithm):
    contributor = "cma4_lliu_saragl_tsuen"
    reads[]
    writes[cma4_lliu_saragl_tsuen.distances]
    
