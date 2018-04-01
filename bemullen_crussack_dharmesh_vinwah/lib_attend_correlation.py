import pandas
from random import shuffle
from math import sqrt

non = pandas.read_csv('non.csv')
stu = pandas.read_csv('stu.csv')

non_x = [] # no students city score
non_y = [] # no students library visits quantity
stu_x = [] # students city score
stu_y = [] # students library visits quantity

non['CTY_SCR_DAY'].apply(lambda row: non_x.append(row) )
non['CTY_SCR_NBR_DY_01'].apply(lambda row: non_y.append(row ))

stu['CTY_SCR_DAY'].apply(lambda row: stu_x.append(row ))
stu['CTY_SCR_NBR_DY_01'].apply(lambda row: stu_y.append(row ))

def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

print('correlation of no students is:')
print('')
print(corr(non_x,non_y))
print('correlation of students is:')
print('')
print(corr(stu_x,stu_y))

