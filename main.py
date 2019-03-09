from SPARQLWrapper import SPARQLWrapper, JSON
import dataset
import queue
import csv
from wikidata.client import Client
import threading
from collections import Counter

import matplotlib.pyplot as plt

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
# sparql.setQuery("""
#     SELECT
#       ?countryLabel ?population ?area ?medianIncome ?age
#     WHERE {
#       ?country wdt:P463 wd:Q458.
#       OPTIONAL { ?country wdt:P1082 ?population }
#       OPTIONAL { ?country wdt:P2046 ?area }
#       OPTIONAL { ?country wdt:P3529 ?medianIncome }
#       OPTIONAL { ?country wdt:P571 ?inception.
#         BIND(year(now()) - year(?inception) AS ?age)
#       }
#       SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
#     }
#     """)
# sparql.setReturnFormat(JSON)
# results = sparql.query().convert()
#
# for result in results["results"]["bindings"]:
#     print(result["countryLabel"]["value"])
def scrape():
    #Define seed/s
    seed = queue.Queue()

KEYS = {
    "spouse": "P26",        #
    "sibling": "P3373",     #
    "mother": "P25",        #
    "father": "P22",        #
    "child": "P40",         #
    "relative": "P1038",    #


    "placeofbirth": "P19",
    "dateofbirth": "P569",  # Distance
    "floruit": "P1317",     # Distance
    "countryofcitizenship": "P27",
    "worklocation": "P937",

    "educatedat": "P69",
    "student": "P802", #Art1 -> Art2        #
    "studentof": "P1066", #Art1 <- Art2     #
    "sponsor": "P859",
    "participantof": "P1344",
    "influencedby": "P737",                 #


    "movement": "P135",
    "genre": "P136",



    "image": "P18"
}
DIRECTED_KEYS = ["spouse","sibling","mother","father","child","relative","student","studentof","influencedby"]
DISTANCE_KEYS = ["dateofbirth","floruit"]

def countRows():
    db = dataset.connect('sqlite:///example.db')
    TOTAL = 25197
    tables = {}
    for k in KEYS:
        r = list(db.query("select count(wid) c from {}".format(k)))
        print("{}\t{}\t{}".format(k,r[0]['c'],r[0]['c']/TOTAL))

def formQuery(artistId):
    b = "SELECT DISTINCT ?" + " ?".join([k for k in KEYS]) + " WHERE {"
    m = ". \n".join(["OPTIONAL {{ wd:{artist} wdt:{id} ?{label_name} }}".format(artist=artistId, id=KEYS[k], label_name=k) for k in KEYS])
    e = "}"
    return b+m+e

def parseEntityUrl(str):
    return str.split("/")[-1]

def getArtist(artistId):
    sparql.setQuery(formQuery(artistId))
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    res = {}
    for result in results["results"]["bindings"]:
        for k in KEYS:
            l = res.get(k,[])
            if k in result and result[k] not in l:
                l.append(result[k])
            res[k] = l
    return res


NUM_URL_WORKERS = 5
artistq = queue.Queue()
dicq = queue.Queue()

cache = {}
def translateId(id):
    if id in cache:
        return cache[id]
    elif id[0] != "Q":
        cache[id] = id
        return id

    sparql.setQuery("""
    SELECT DISTINCT ?id ?idLabel
    WHERE
    {{
      bind (wd:{} AS ?id)
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,es,it". }}
      
    }}
    LIMIT 1""".format(id))
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    k = results["results"]["bindings"][0]['idLabel']['value']
    cache[id] = "{} ({})".format(k,id)
    return cache[id]


def worker1():
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

    while True:
        artistId = artistq.get()
        if artistId is None:
            break
        sparql.setQuery(formQuery(artistId))
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        res = {'artistId': artistId}
        for result in results["results"]["bindings"]:
            for k in KEYS:
                l = res.get(k, [])
                if k in result and result[k] not in l:
                    l.append(result[k])
                res[k] = l

        dicq.put(res)

    dicq.put(None)



def worker2():
    db = dataset.connect('sqlite:///example.db')
    table = db['pending_artists']

    i = 0
    for artist in table:
        artistq.put(artist['wid'])

        # i += 1
        # if i == 10:
        #     break

    for i in range(NUM_URL_WORKERS):
        artistq.put(None)

def main1():
    db = dataset.connect('sqlite:///example.db')
    tables = {}
    for k in KEYS:
        tables[k] = db[k]
        #tables[k].drop()

    threads = []
    # Artist adder
    t = threading.Thread(target=worker2())
    t.start()
    threads.append(t)

    # URL workers
    for i in range(NUM_URL_WORKERS):
        t = threading.Thread(target=worker1)
        t.start()
        threads.append(t)

    i=0
    none_count = 0
    while True:
        item = dicq.get()
        if item is None:
            none_count += 1
            if none_count == NUM_URL_WORKERS:
                break
            else:
                continue

        for k in KEYS:
            if k != "image":
                els = [dict(k=parseEntityUrl(el['value']) if el['type'] == 'uri' else el['value'], wid=item['artistId']) for el in item[k]]
            else:
                els = [dict(k=item[k][0]['value'],wid=item['artistId'])]
            tables[k].insert_many(els)
        i += 1
        print(i)

    for t in threads:
        t.join()

    # i = 0
    # for artist in table:
    #     dic = getArtist(artist['wid'])
    #     for k in dic:
    #         els = [dict(k=parseEntityUrl(el['value']) if el['type'] == 'uri' else el['value'], wid=artist['wid']) for el in dic[k]]
    #         tables[k].insert_many(els)
    #     print(i)
    #     i+= 1


    #table.drop()
    #table.insert(dict(name="John Doe", age=46))
    # res = []
    # with open('tables/query.csv') as csvfile:
    #     reader = csv.reader(csvfile,delimiter=',')
    #     next(reader)
    #     getId = (lambda x: x.split('/')[-1])
    #     for row in reader:
    #         res.append(dict(wid=getId(row[0]),name=row[1],visited=False))
    # table.insert_many(res)
    # print("Hola")

def main2():
    db = dataset.connect('sqlite:///example.db')
    tables = {}
    for k in KEYS:
        tables[k] = db[k]

    edges = {}

    statement = 'SELECT k, COUNT(k) c FROM {} GROUP BY k;'
    for t in tables:
        for row in db.query(statement.format(t)):
            kid = row['k']
            c = row['c']
            if(c>1):
                table = tables[t]
                res = list(table.find(k=kid,order_by='wid'))
                res_size = len(res)
                for i in range(res_size):
                    iid = res[i]['wid']
                    for j in range(i,res_size):
                        jid = res[j]['wid']
                        if iid == jid:
                            continue
                        l = edges.get(iid, [])
                        # j ID, KIND, kind ID
                        #l.append((jid, t, kid))
                        l.append(jid)

                        edges[iid] = l
        print(t)

    from collections import Counter
    with open('tables/edges.csv','w') as csvfile:
        writer = csv.writer(csvfile,delimiter=',')
        #writer.writerow(['Source', 'Target', 'Type', 'Weigth'])
        for wid in edges:
            e = Counter(edges[wid])
            for widp in e:
                writer.writerow([wid,widp,e[widp]])

def main_dir():
    db = dataset.connect('sqlite:///example.db')
    tables = {}
    for k in KEYS:
        tables[k] = db[k]

    edges = {}


    statement = 'SELECT k, COUNT(k) c FROM {} GROUP BY k;'
    for t in DIRECTED_KEYS:
        for row in tables[t]:
            iid = row['wid']
            kid = row['k']
            #if k is artist
            if(tables[t].count(wid=kid) > 0):
                l = edges.get(iid,[])
                l.append(kid)
                edges[iid] = l
        print(t)

    from collections import Counter
    with open('tables/edges2.csv','w') as csvfile:
        writer = csv.writer(csvfile,delimiter=',')
        writer.writerow(['Source', 'Target', 'Type', 'Weigth'])
        for wid in edges:
            e = Counter(edges[wid])
            for widp in e:
                writer.writerow([wid,widp,"Directed",e[widp]])

from math import log
def main2dist():
    db = dataset.connect('sqlite:///example.db')
    tables = {}
    for k in KEYS:
        tables[k] = db[k]

    edges = {}
    res= list(db.query("select wid, cast(substr(k,1,4) as decimal) year from date order by year;"))
    len_res = len(res)
    N = 15
    lN = log(N+1)
    with open('tables/edges_undir_year.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['Source', 'Target', 'Type', 'Weight'])
        for i in range(len_res):
            iid = res[i]['wid']
            iage = res[i]['year']
            for j in range(i+1,len_res):
                jid = res[j]['wid']
                jage = res[j]['year']
                if jage - iage > N:
                    break
                dist = log(N+1 - (jage-iage))/lN
                writer.writerow([iid,jid,'Undirected',dist])
            print(i/len_res)


def filterDist():
    ids = set()
    with open('tables/eigen030.csv','r') as csvfile:
        reader = csv.reader(csvfile,delimiter=',')
        next(reader)
        for row in reader:
            if float(row[3]) > 0.4:
                ids.update([row[0]])

    a = 0
    with open('tables/edges_undir_year.csv') as disfile:
        with open('tables/edges_undir_year2.csv', 'w') as disfile2:
            reader = csv.reader(disfile,delimiter=',')
            writer = csv.writer(disfile2,delimiter=',')
            writer.writerow(next(reader))

            for row in reader:
                if row[0] in ids or row[1] in ids:
                    writer.writerow(row)

def reverseWeight(path):
    outpath = path.split('.')
    outpath = '.'.join(outpath[:-1]+['inv']+[outpath[-1]])
    with open(path,'r') as inputfile:
        #with open(outpath,'w') as outputfile:
        for row in inputfile:
            #outputfile.write(row)
            if(row == "*Edges\n"):
                break
        i=0;
        for row in inputfile:
            l = row.split(' ')
            l[2] = str(1.0/float(l[2]))
            if l[0] == l[1]:
                print(l[0])
            #outputfile.write(' '.join(l)+'\n')
        print(i)

def pajeckToCSV(labelspath,pajeckpath):
    outpath = pajeckpath.split('.')
    outpath = '.'.join(outpath[:-1]+['csv'])
    labels = {}
    labels2 = {}
    with open(labelspath) as inputfile:
        reader = csv.reader(inputfile, delimiter=',')
        next(reader)
        i=1
        for row in reader:
            labels[str(i)] = row[0]
            i+=1

    with open(pajeckpath) as inputfile:
        next(inputfile)
        for row in inputfile:
            if (row == "*edges\n"):
                break

        with open(outpath,'w') as outputfile:
            writer = csv.writer(outputfile,delimiter=',')
            writer.writerow(["Source","Target","Type","Weight"])
            s = set()
            s2 = set()
            for row in inputfile:
                l = row.split(" ")
                st = l[0]+" "+l[1]
                st2 = labels[l[0]] + " " + labels[l[1]]
                if st in s:
                    print(st)
                if st2 in s2:
                    print(st,st2)
                s.update([st])
                s2.update([st2])
                writer.writerow([labels[l[0]],labels[l[1]],"Undirected",float(l[2])])

def convToLov():
    inpath = "tables/edges_undir_year2.csv"
    outpath = "tables/lov/e2.csv"
    infile = open(inpath)
    outfile1 = open(outpath,'w')
    outfile2 = open(outpath+".index", 'w')
    incsv = csv.reader(infile, delimiter=",")
    outcsv1 = csv.writer(outfile1, delimiter=" ")
    outcsv2 = csv.writer(outfile2, delimiter=" ")
    next(incsv)
    table = {}
    counter = 0
    for row in incsv:
        id1 = row[0]
        id2 = row[1]
        w = row[3]

        if id1 not in table:
            table[id1] = counter
            outcsv2.writerow([counter,id1])
            counter += 1


        if id2 not in table:
            table[id2] = counter
            outcsv2.writerow([counter, id2])
            counter += 1

        outcsv1.writerow([table[id1],table[id2],w])


ANAKEYS = {
    # "placeofbirth": "P19",
    #"dateofbirth": "P569",  # Distance
    #"floruit": "P1317",     # Distance
    "date": "0",     # Distance
    "countryofcitizenship": "P27",
    # "worklocation": "P937",
    #
    # "educatedat": "P69",
    # "student": "P802", #Art1 -> Art2        #
    # "studentof": "P1066", #Art1 <- Art2     #
    # "sponsor": "P859",
    # "participantof": "P1344",
    # "influencedby": "P737",                 #
    #
    #
    # "movement": "P135",
    # "genre": "P136",
}

def analyzeGroup(lst,size):
    keys = [k for k in ANAKEYS]
    res = {}

    db = dataset.connect('sqlite:///example.db')
    tables = {}
    for k in keys:
        tables[k] = db[k]

    for k in keys:
        values = [el['k'] for el in tables[k].find(wid=lst)]
        if k == "date":
            values = [k[:2] for k in values]
        res[k] = {'v': Counter(values),'len': len(values)}

    country = ""
    century = ""
    for k in keys:
        n = min(3,len(res[k]['v']))
        print(k + "".join(["\t{}\t{}".format(
            translateId(res[k]['v'].most_common()[i][0]),
            res[k]['v'].most_common()[i][1] / size
        ) for i in range(n)]))
        if n > 0 and k == "countryofcitizenship":
            country = translateId(res[k]['v'].most_common()[0][0])
        if n > 0 and k == "date":
            century = int(translateId(res[k]['v'].most_common()[0][0]))+1

    return "{} (S.{})".format(" ".join(country.split(" ")[:-1]),century)



def clusterAnalysis():


    clusters = {}

    inpath = "/home/guillermo/PycharmProjects/RSC_Art/tables/analysis/NODES_min_connected.net.csv"
    infile = open(inpath)
    incsv = csv.reader(infile)

    next(incsv)
    for row in incsv:
        # id = 0
        # eigen = 5
        # class = 6

        rid = row[0]
        reigen = float(row[5])
        rclass = row[6]

        c = clusters.get(rclass,{'eig':[], 'ids':[], 'n': 0})

        c['eig'].append(reigen)
        c['ids'].append(rid)
        c['n'] += 1

        clusters[rclass] = c

    res = [clusters[e]['eig'] for e in clusters]
    k = [int(e) for e in clusters]
    srtd = sorted(zip([clusters[str(i)]['n'] for i in k],k,res),reverse=True)
    for k in [k for _,k,_ in srtd]:
        print(k,clusters[str(k)]['n'])
    plt.title("Cluster eigenvalue")
    plt.boxplot([x for _,_,x in srtd],labels=[str(k)+' ({})'.format(clusters[str(k)]['n']) for _,k,_ in srtd])
    plt.yscale('log')
    plt.xticks(rotation=60)
    plt.show()

    #bubble plot
    x = [e[0] for e in srtd]
    y = [sum(e[2])/e[0] for e in srtd]
    z = x

    eigen_log = list(map(lambda x: log(x,10),y))
    log_range = max(eigen_log) - min(eigen_log)
    log_min = min(eigen_log)


    plt.scatter(x,y,s=z, c="red", alpha=0.4)
    plt.yscale('log')
    plt.ylim((1e-5,0.9))
    plt.xlim((-50,3500))
    plt.title("Cluster size/eigenvalue")
    plt.show()

    titles = {}
    for c in clusters:
        print("Cluster {} (Eigenvalue: {}) (Size: {}):".format(c, sum(clusters[c]['eig'])/clusters[c]['n'],clusters[c]['n']))
        titles[c] = analyzeGroup(clusters[c]['ids'],clusters[c]['n'])

    idC = {}
    for c in clusters:
        for id in clusters[c]['ids']:
            idC[id] = int(c)

    inpath = "/home/guillermo/PycharmProjects/RSC_Art/tables/analysis/EDGES_min_connected.net.csv"
    infile = open(inpath)
    incsv = csv.reader(infile)

    import numpy as np

    weights = np.zeros((len(clusters),len(clusters)),dtype=np.float)

    next(incsv)
    for row in incsv:
        src = idC[row[0]]
        dst = idC[row[1]]
        w = float(row[6])

        weights[src][dst] += w
        weights[dst][src] += w

    outpath = "/home/guillermo/PycharmProjects/RSC_Art/tables/analysis/EDGES_clusters_net.csv"
    outfile = open(outpath,'w')
    outcsv = csv.writer(outfile)
    outcsv.writerow(["Source","Target","Type","Weight"])
    for i in range(len(clusters)):
        for j in range(i,len(clusters)):
            if i != j  and weights[i][j] > 0:
                outcsv.writerow([i,j,"Undirected",weights[i][j]])

    outpath = "/home/guillermo/PycharmProjects/RSC_Art/tables/analysis/NODES_clusters_net.csv"
    outfile = open(outpath,'w')
    outcsv = csv.writer(outfile)
    outcsv.writerow(["Id","Label","size","mean_eigen","log_norm","highest_id","highest_label","highest_eigen"])
    for c in clusters:
        e = sum(clusters[c]['eig']) / clusters[c]['n']
        maxe = max(clusters[c]['eig'])
        hid = clusters[c]['ids'][clusters[c]['eig'].index(maxe)]
        outcsv.writerow([
            str(c),
            titles[c],
            clusters[c]['n'],
            e,
            (log(e,10)-log_min)/log_range,
            hid,
            " ".join(translateId(hid).split(" ")[:-1]),
            maxe
        ])

def filterSpanish():
    db = dataset.connect('sqlite:///example.db')

    l = list(db.query('select wid from countryofcitizenship where k="Q29"'))
    l = set([e['wid'] for e in l])

    ifile = open('/home/guillermo/PycharmProjects/RSC_Art/tables/all.csv')
    incsv = csv.reader(ifile)

    ofile = open('/home/guillermo/PycharmProjects/RSC_Art/tables/spanish.csv','w')
    outcsv = csv.writer(ofile)

    outcsv.writerow(next(incsv))

    for r in incsv:
        if r[0] in l and r[1] in l:
    #    if r[0] in l:
             outcsv.writerow(r)


import json
import numpy as np
import igraph as ig
import plotly.plotly as py
import plotly.graph_objs as go
import plotly
from itertools import chain
import urllib

def paintGraph():
    VISUALIZE = False
    j = json.load(open("/home/guillermo/PycharmProjects/RSC_Art/out.json"))
    nodes = j['nodes']
    means = [np.mean([n[k] for n in nodes]) for k in ['x', 'y', 'z']]
    means = {"x": means[0], "y": means[1], "z": means[2]}

    coords = np.array([[n[k] - means[k] for n in nodes] for k in ['x', 'y', 'z']]).transpose()
    for i in range(len(coords)):
        coords[i] = coords[i] / np.sqrt(np.sum(np.power(coords[i],2)))
    coords = coords.transpose()

    edges = [[e['source'], e['target']] for e in j['edges']]

    nc = {}
    ids = [n['id'] for n in j['nodes']]
    for i, id in enumerate(ids):
        nc[id] = coords.transpose()[i]

    #Transform nodes
    innodefile = open('/home/guillermo/PycharmProjects/RSC_Art/tables/minispanish_nodes.csv')
    incsv = csv.reader(innodefile)
    next(incsv)
    inlist = list(incsv)
    # outnodefile = open('/home/guillermo/PycharmProjects/RSC_Art/tables/unity_nodes.csv','w')
    # outcsv = csv.writer(outnodefile)
    # outcsv.writerow(["id", "label", "x", "y", "z", "size"])
    #
    # size = np.array([-np.log(float(e[5]))/np.log(0.8657) for e in inlist])
    # size -= np.min(size)
    # size /= np.max(size)
    db = dataset.connect('sqlite:///example.db')
    table = db['image']

    for i,r in enumerate(inlist):
        id = r[0]
        print(id)
        url = table.find_one(wid=id)['k']
        ext = url.split('.')[-1]
        out_path = "imgs/{}.{}".format(id,ext)
        urllib.request.urlretrieve(url,out_path)

        # label = r[1]
        # [x,y,z] = nc[id]
        # s = size[i]
        #
        # outcsv.writerow([id,label,x,y,z,s])







    if VISUALIZE:
        Xn = list(coords[0])
        Yn = list(coords[1])
        Zn = list(coords[2])

        Xe = list(chain(*[[nc[s][0], nc[t][0], None] for [s, t] in edges]))
        Ye = list(chain(*[[nc[s][1], nc[t][1], None] for [s, t] in edges]))
        Ze = list(chain(*[[nc[s][2], nc[t][2], None] for [s, t] in edges]))

        trace1 = go.Scatter3d(x=Xe,
                              y=Ye,
                              z=Ze,
                              mode='lines',
                              line=dict(color='rgb(125,125,125)', width=1),
                              hoverinfo='none'
                              )
        trace2 = go.Scatter3d(x=Xn,
                              y=Yn,
                              z=Zn,
                              mode='markers',
                              name='actors',
                              marker=dict(symbol='circle',
                                          size=6,

                                          colorscale='Viridis',
                                          line=dict(color='rgb(50,50,50)', width=0.5)
                                          ),

                              hoverinfo='none'
                              )
        axis = dict(showbackground=False,
                    showline=True,
                    zeroline=True,
                    showgrid=True,
                    showticklabels=True,
                    title=''
                    )
        layout = go.Layout(
            title="Network of coappearances of characters in Victor Hugo's novel<br> Les Miserables (3D visualization)",
            width=1000,
            height=1000,
            showlegend=False,
            scene=dict(
                xaxis=dict(axis),
                yaxis=dict(axis),
                zaxis=dict(axis),
            ),
            margin=dict(
                t=100
            ),
            hovermode='closest',
            annotations=[
                dict(
                    showarrow=False,
                    text="Data source: <a href='http://bost.ocks.org/mike/miserables/miserables.json'>[1] miserables.json</a>",
                    xref='paper',
                    yref='paper',
                    x=0,
                    y=0.1,
                    xanchor='left',
                    yanchor='bottom',
                    font=dict(
                        size=14
                    )
                )
            ], )
        data = [trace1, trace2]
        #data = [trace1]
        fig = go.Figure(data=data, layout=layout)
        plotly.offline.plot(fig, filename='Les-Miserables')

import networkx as nx

def shortestPaths():
    infile = open('/home/guillermo/PycharmProjects/RSC_Art/tables/unity_edges.csv')
    incsv = csv.reader(infile)
    outfile = open('paths.csv','w')
    outcsv = csv.writer(outfile)
    next(incsv)
    paths = ["{} {}".format(e[0],e[1]) for e in incsv]

    G = nx.parse_edgelist(paths,nodetype=str)
    for i,d in enumerate(nx.all_pairs_shortest_path(G)):
        print(i)
        for p in d[1]:
            s = d[0]
            t = p
            n = len(d[1][p])
            l = d[1][p]
            outcsv.writerow([s,t,n]+l)

from os import listdir
from os.path import isfile, join

from PIL import Image
from resizeimage import resizeimage

def resizeImage():
    path = "/home/guillermo/PycharmProjects/RSC_Art/imgs"
    files = [join(path,f) for f in listdir(path) if isfile(join(path,f))]
    a = 0
    maxs = 1024
    for fp in files:
        with open(fp,'r+b') as f:
            with Image.open(f) as image:
                r = maxs / image.size[0]
                rim = image.resize((maxs,int(image.size[1]*r)))
                rim.save(join(path,fp))


def main():
    db = dataset.connect('sqlite:///example.db')
    table = db['pending_artists']


    with open('tables/nodes.csv','w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['Id','Label'])
        for artist in table:
            writer.writerow([artist['wid'],artist['name']])





if __name__ == "__main__":
    #main2dist()
    #filterDist()
    #reverseWeight("/media/guillermo/7bfc690b-85c4-405b-adf0-e198ab068f37/home/guillermo/UGR/RSC/P3/MST_Practical/edges_min.net")
    #pajeckToCSV("tables/NODES_min_connected.net.csv","tables/res.net")
    #convToLov()
    #clusterAnalysis()
    #countRows()
    #filterSpanish()
    #paintGraph()
    #shortestPaths()
    resizeImage()
