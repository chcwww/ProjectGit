import numpy as np
import pandas as pd
import time
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

dataT = pd.read_csv(r'C:\\R1091\\new\\RFMdata.csv')
dataT.head()
dataTest = dataT.loc[:, ["amount", "freq", "dateBeforOffer"]]
labels = dataT.loc[:, ["repeater"]]


scaler = StandardScaler()
scaler.fit(dataTest)
stData = scaler.transform(dataTest)

silhouette = []
for i in range(2, 30):
    tryKmeans = KMeans(n_clusters = i, n_init = 10).fit(stData)
    silhouette.append(silhouette_score(stData, tryKmeans.labels_))

plt.plot(range(2, 30), silhouette)
plt.show()

sum(tryKmeans.labels_ == dataT["repeater"])






fig = plt.figure(figsize=(8,3))
colors = ['#4EACC5', '#FF9C34', '#4E9A06']

# original data
ax = fig.add_subplot(1,2,1)
fig.subplots_adjust(left=0.02, right=0.98, bottom=0.05, top=0.9)
row, _ = np.shape(X)
for i in range(row):
    ax.plot(X[i, 0], X[i, 1], '#4EACC5', marker='.')

ax.set_title('Original Data')
ax.set_xticks(())
ax.set_yticks(())

# compute clustering with K-Means
k_means = KMeans(init='k-means++', n_clusters=5, n_init=10)
t0 = time.time()
k_means.fit(X)
t_batch = time.time() - t0

k_means_cluster_centers = np.sort(k_means.cluster_centers_, axis=0)
k_means_labels = pairwise_distances_argmin(X, k_means_cluster_centers)

# K-means
ax = fig.add_subplot(1, 2, 2)
for k, col in zip(range(n_clusters), colors):
    my_members = k_means_labels == k		# my_members是布尔型的数组（用于筛选同类的点，用不同颜色表示）
    cluster_center = k_means_cluster_centers[k]
    ax.plot(X[my_members, 0], X[my_members, 1], 'w',
            markerfacecolor=col, marker='.')	# 将同一类的点表示出来
    ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
            markeredgecolor='k', marker='o')	# 将聚类中心单独表示出来
ax.set_title('KMeans')
ax.set_xticks(())
ax.set_yticks(())
plt.text(-3.5, 1.8, 'train time: %.2fs\ninertia: %f' % (t_batch, k_means.inertia_))

plt.show()





X = dataTest
scaler = StandardScaler()
scaler.fit(X)
X = scaler.transform(X)


# with standardization
covX = np.dot(X.T, X) / (X.shape[0] - 1)

eigvec, eigval, V = np.linalg.svd(covX)

fig, ax = plt.subplots(nrows=1,ncols=2,figsize=(9,4))

x = np.arange(1, 1+len(eigval))
ax[0].plot(x, eigval, marker = 's')
ax[0].set_ylabel('$\lambda$')
ax[0].set_title('Screen plot')
ax[0].set_xlabel('Principal Compnent')
ax[0].grid(True)

ax[1].bar(x, eigval, color = colors)
ax2 = ax[1].twinx()
ax2.plot(x, eigval.cumsum()/eigval.sum()*100, \
         marker = '*', color = 'crimson', lw = 1.5)
ax2.tick_params(axis = 'y', colors = 'saddlebrown')
ax2.yaxis.set_major_formatter(PercentFormatter())
ax[1].set_title('Pareto Plot')
ax[1].set_xlabel('Principal Component')
ax[1].set_ylabel('Variance Explanined')
plt.show()


Xpca = np.dot(X, eigvec)
Xpca_reduced = Xpca[:, :2]
labels = labels.loc[Xpca_reduced[:,0]>-100]
Xpca_reduced = Xpca_reduced[Xpca_reduced[:,0]>-100, :]


col = ['palevioletred', 'darkcyan', 'tan']
marker = ['o', 'h', 'd']
for i, _ in zip(['t', 'f'], [1, 2]):
    plt.plot(Xpca_reduced[labels == i, 0], Xpca_reduced[labels == i, 1], marker[_-1], \
            alpha = 0.7, color = col[_-1])
plt.grid(True)
plt.title('$Z_1$ vs. $Z_2$')
plt.xlabel('$Z_1$')
plt.ylabel('$Z_2$')
plt.legend(['factory1', 'factory2', 'factory3'], loc = 'upper right')
plt.axis('equal')
plt.show()







