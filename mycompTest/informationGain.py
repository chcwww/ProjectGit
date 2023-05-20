from scipy.stats import entropy
import pandas as pd 


dirThis = 'C:\\Users\\chcww\\Downloads\\'

testHistory = pd.read_csv(dirThis + 'testHistory.csv')

a = pd.read_csv(dirThis + 'trainEncode.csv')
b = pd.read_csv(dirThis + 'targetEncode.csv')
train = a.drop(columns = ['Unnamed: 0', 'id', 'chain', 'market', 'offerdate'])
target = b['repeater']

c = pd.read_csv(dirThis + 'trainMTDF.csv')
targetMt = c['repeater']
trainMt = c.drop(columns = ['Unnamed: 0', 'id', 'chain', 'market', 'repeater'])
# test = pd.read_csv(dirThis + 'testEncode.csv')

d = dirThis + 'projectData\\'

a = pd.read_csv(d + 'trainEncodeBIG.csv')
b = pd.read_csv(d + 'targetEncodeBIG.csv')
k = pd.read_csv(d + 'trainStdBIG.csv')
trainBig = a.drop(columns = ['Unnamed: 0', 'id', 'chain', 'market', 'offerdate'])
trainStdBig = k.drop(columns = ['Unnamed: 0', 'id', 'chain', 'market', 'offerdate'])
targetBig = b['repeater']

c = pd.read_csv(d + 'trainMTDFBIG.csv')
targetMtBig = c['repeater']
trainMtBig = c.drop(columns = ['Unnamed: 0', 'id', 'chain', 'market', 'repeater'])
# test = pd.read_csv(dirThis + 'testEncode.csv')



def informationGain(members, split) :
    entropy_before = entropy(members.values_counts(normalize = True))
    split.name = 'split'
    members.name = 'members'
    grouped_distrib = members.grouby(split) \
        .value_counts(normalize = True) \
        .reset_index(name = 'count') \
        .pivot_table(index = 'split', columns = 'members', values = 'count').fillna(0)
    entropy_after = entropy(grouped_distrib, axis = 1)
    entropy_after *= split.value_counts(sort = False, normalize = True)
    return entropy_before - entropy_after.sum()




from sklearn.datasets import fetch_20newsgroups
from sklearn.feature_selection import mutual_info_classif
from sklearn.feature_extraction.text import CountVectorizer

categories = ['talk.religion.misc',
              'comp.graphics', 'sci.space']
newsgroups_train = fetch_20newsgroups(subset='train',
                                      categories=categories)

X, Y = newsgroups_train.data, newsgroups_train.target
cv = CountVectorizer(max_df=0.95, min_df=2,
                                     max_features=10000,
                                     stop_words='english')
X_vec = cv.fit_transform(X)



res = dict(zip(train.columns,
               mutual_info_classif(trainStdBig, targetBig)
               ))

sortDict = sorted(res.items(), key=lambda x:x[1], reverse = True)

for i, j in enumerate(sortDict) :
    print(str(i+1) + ' - ' + str(j))

select = []
for j in sortDict :
    select.append(j[0])

print(select)