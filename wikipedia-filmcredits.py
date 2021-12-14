# libraries and functions.

from bs4 import BeautifulSoup, SoupStrainer
from requests_html import HTMLSession
import datetime
import json
import pandas
import pathlib
import pydash
import requests
import time

def value_extract(row, col):

    # extract dictionary value. 

    return pydash.get(row[col], 'value')    
    
def sparql_query(query, service):

    # send sparql request, and formulate results into a dataframe. 

    r = requests.get(service, params = {'format': 'json', 'query': query})
    data = pydash.get(r.json(), 'results.bindings')
    data = pandas.DataFrame.from_dict(data)
    for x in data.columns:    
        data[x] = data.apply(value_extract, col=x, axis=1)
    return data

def wikipedia_to_wikidata(link):

    # retrieve wikidata id from en wikipedia page title. 

    if 'wiki' in link:
      query = 'https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&titles='
      query += str(link.replace('/wiki/', ''))
      query += '&format=json'
      r = requests.get(query)
      if r.status_code == 200:
          if str(r.text)[0] == '{':
              r = json.loads(r.text)
              for x in pydash.get(r, 'query.pages'):
                  return pydash.get(r, f'query.pages.{x}.pageprops.wikibase_item')

def find_join(nodes):

  # identify joining symbol.

  for j in [': ', ' as ', ' - ', ' – ']:
    res = sum([bool(x.find(j)+1) for x in nodes[1:]])/len(nodes)
    if res >= 0.85:
      return j
  else:
    return None

def extract_actor(source):

  # extract wikidata entity from wikipedia claim.

  if '</a>' in source:
    for link in BeautifulSoup(source, 'html.parser', parse_only=SoupStrainer('a')):
      if link.has_attr('href'):
        return wikipedia_to_wikidata(link['href'])

# sparql query a list of wikidata films and respective english wikipedia pages.

film_dataframe = sparql_query("""
    SELECT DISTINCT ?wikidata ?wikipedia WHERE {
        ?wikidata wdt:P31 wd:Q11424
        OPTIONAL {
            ?wikipedia schema:about ?wikidata.
            ?wikipedia schema:inLanguage "en".
            ?wikipedia schema:isPartOf <https://en.wikipedia.org/>
        }}""", 'https://query.wikidata.org/sparql')

film_dataframe['wikidata'] = film_dataframe['wikidata'].str.split('/').str[-1]
film_dataframe = film_dataframe.loc[~film_dataframe.wikipedia.isin([None])]

# parse cast lists, including pulling actor wikidata links where possible.

film_dict = film_dataframe.to_dict('records')

extant_log = pathlib.Path.cwd() / 'processed.txt'
if extant_log.exists():
  with open(extant_log) as extant:
    extant = [x for x in extant.read().split('\n') if 'Q' in x]
    film_dict = [x for x in film_dict if x['wikidata'] not in extant]

film_dict = film_dict[:200]

commence = datetime.datetime.now()
for i in range(len(film_dict)):

  time.sleep(1)
  t = (datetime.datetime.now()-commence)/(i+1)
  time_to_finish = (((t)*(len(film_dict)))+commence).strftime("%Y-%m-%d %H:%M:%S")
  print(f'processing: {i+1} of {len(film_dict)}; eta {time_to_finish}.')

  film_id = pydash.get(film_dict[i], 'wikidata')
  page = HTMLSession().get(str(pydash.get(film_dict[i], 'wikipedia'))).text
  cast_block = [x for x in page.split('<h2>') if 'id="Cast"' in str(x)]
  if len(cast_block) == 1:
    cast_block = cast_block[0]
    cast_nodes = [x for x in cast_block.split('<li>')]
    join_symbol = find_join(cast_nodes)
    if join_symbol:
      for credit in cast_nodes[1:]:
        if isinstance(credit, str):
          if join_symbol in credit:
            join_location = credit.find(join_symbol)
            entity = extract_actor(credit[:join_location]) 
            char = credit[join_location+len(join_symbol):]
            for char_split in ['</li>', '(credited', '(billed', ',', ':', ';']:
              char = char.split(char_split)[0].strip()
            char = BeautifulSoup(char, 'lxml').text
            if entity:
              filename = datetime.datetime.now().strftime('%Y-%m-%d')
              datapath = pathlib.Path.cwd() / 'data' / f'{filename}.txt'
              datapath.parents[0].mkdir(parents=True, exist_ok=True)
              with open(datapath, 'a') as claim:
                claim.write(f'{film_id} {entity} {char}\n')

  with open(pathlib.Path.cwd() / 'processed.txt', 'a') as proc:
    proc.write(film_id+'\n')