import numpy as np
import sys
from sklearn.cluster import DBSCAN
from sklearn import metrics
from geopy.distance import vincenty
from scipy.spatial.distance import cosine, euclidean
from collections import Counter
import time

from sklearn.cluster import AgglomerativeClustering

try:
   import cPickle as pickle
except:
   import pickle

def d(a,b):
    cos = cosine(a[4:], b[4:])
    euc = euclidean(a[3:], b[3:])
    vinc = (float(vincenty((a[0], a[1]), (b[0], b[1])).km))

    #print("text:" + str(cos))
    #print("time:" + str(euc))
    #print("spatial:"+ str(vinc))
    return cos*5 #+ euc + vinc*0.01

features = np.load("data/features.pick")

dat = features[0:500,:]
tweets = pickle.load( open("data/pp_tweets.pick", "rb" ) )

for i in range(4,24):
    print(d(dat[4,:], dat[i,:]))
    print(" ".join(tweets[i]))
    print(" ")

#sys.exit(1)

##############################################################################
print("Compute Clusters")
db = DBSCAN(eps=0.13, min_samples=5, metric=d, algorithm='ball_tree').fit(dat)
#db = AgglomerativeClustering(n_clusters=10, affinity=d, linkage="complete").fit(dat)
core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
#core_samples_mask[db.core_sample_indices_] = True
labels = db.labels_

# # Number of clusters in labels, ignoring noise if present.
n_clusters_ =  0 #len(set(labels)) - (1 if -1 in labels else 0)


# Plot result
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import matplotlib.cm as cm

unique_labels = set(labels)
colors = plt.cm.Spectral(np.linspace(0, 1, len(unique_labels)))


####map config
bbox = {
    'lon': -5.23636,
    'lat': 53.866772,
    'll_lon': -10.65073,
    'll_lat': 49.16209,
    'ur_lon': 1.76334,
    'ur_lat': 60.860699
}

m = Basemap(
    projection='merc', lon_0=bbox['lon'], lat_0=bbox['lat'], resolution="h",
    llcrnrlon=bbox['ll_lon'], llcrnrlat=bbox['ll_lat'],
    urcrnrlon=bbox['ur_lon'], urcrnrlat=bbox['ur_lat'])
m.drawcoastlines()#
m.fillcontinents(color='gray', lake_color='aqua',zorder=5)

m.drawmapboundary(fill_color='aqua')
####end map config

clusterLabels = db.labels_

for k, col in zip(unique_labels, colors):
    if k == -1:
        # Black used for noise.
        col = 'k'
    class_member_mask = (labels == k)
    #print(k)
    #print(class_member_mask)
    xy = dat[class_member_mask, 0:3]
    numOfDistinctUsers = len(set(xy[:,2]))

    
    x1, y1 = m(xy[:, 0],xy[:, 1])
    if k == -1 or numOfDistinctUsers < 2:
        pass
        #m.scatter(x1, y1, s=4, marker="o", c=col, alpha=0.5, zorder=10)
    else:

        #plot k to map at position 
        wordsOfCluster = []
        
        xtxt = 0
        ytxt = 0

        for i, label in enumerate(clusterLabels):
            
            if label == k:
                #das ist der tweet
                print(' '.join(tweets[i]))
                if xtxt == 0 and ytxt == 0:
                    xtxt, ytxt = m(dat[i,0], dat[i,1])
                for word in tweets[i]:
                    wordsOfCluster.append(word)

        #evaluate clusters
        wfcnt = Counter(wordsOfCluster)
        
        print(str(n_clusters_) + ". Cluster, with " + str(xy.shape[0]) + " tweets, " + str(numOfDistinctUsers) + " distinct users, " +
            "the most freq. words are:")

        print(wfcnt.most_common()[0:15])
        print(" ------------- ")
        print(" ")
        if wfcnt.most_common()[0][1]/len(x1) > 0.2:
            #plot points
            m.scatter(x1, y1, s=50, marker="o", c=col, alpha=1, zorder=10)
            
            #print cluster numbers on plot      
            plt.text(xtxt, ytxt, str(k), fontsize=12,fontweight='bold',
                ha='left',va='bottom',color='k', zorder=15)
            n_clusters_ += 1

plt.title("Clustered tweets from GB %d" % n_clusters_)
plt.savefig(time.strftime("plots/test-%h-%m-%s-%d-%m-%Y.png"))
plt.show()



