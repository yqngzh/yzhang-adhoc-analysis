import numpy as np
from functools import partial

sig1 = [5, 8, 10, 4, 7, 1, 8, 7, 1, 6, 4] # cols
sig2 = [6, 5, 6, 5, 7, 10, 3, 5, 3, 6, 6] # rows 

N = len(sig1)
mat = np.zeros((N, N), dtype=np.int64)

mat[0, 0] = np.abs(sig1[0] - sig2[0])

for row in range(1, N):
    mat[row, 0] = np.abs(sig2[row] - sig1[0]) + mat[row-1, 0]

for col in range(1, N):
    mat[0, col] = np.abs(sig2[0] - sig1[col]) + mat[0, col-1]

for r in range(1, N):
    for c in range(1, N):
        prev = np.min([mat[r-1, c], mat[r, c-1], mat[r-1, c-1]])
        mat[r,c] = np.abs(sig2[r] - sig1[c]) + prev

mat[0, :]


# path = [1,1,2,2,3,3,4,6,8,10,11,16,16,18]
path = [1,1,3,7,8,8,11,13,16,18,20,20,22]
x = 0
for idx in range(len(path)-1):
    x += (path[idx] - path[idx + 1]) ** 2
print(x)


def gaussian(x, mu, sigma):
    res = 1. / np.sqrt(2 * np.pi * (sigma ** 2))
    res = res * np.exp( - ((x - mu) ** 2) / (2 * (sigma ** 2)) )
    return res

pa = partial(gaussian, mu=0, sigma=1.8)
pb = partial(gaussian, mu=-1, sigma=1.6)
pc = partial(gaussian, mu=1, sigma=1.3)

0.7 * pa(0.3) * 0.1
0.3 * pb(0.3) * 0.2

0.7 * pa(0.3) * 0.5 * pa(-0.1) * 0.1
0.7 * pa(0.3) * 0.3 * pb(-0.1) * 0.2
0.7 * pa(0.3) * 0.1 * pc(-0.1) * 0.5

0.5 * pa(-0.1) * 0.5 * pa(0.3) * 0.1
0.5 * pa(-0.1) * 0.3 * pb(0.3) * 0.2
0.3 * pb(-0.1) * 0.2 * pc(0.3) * 0.5 

0.7 * pa(0.3) * 0.5 * pa(-0.1) * 0.5 * pa(0.3) * 0.5 * pa(0.5) * 0.1
0.7 * pa(0.3) * 0.5 * pa(-0.1) * 0.5 * pa(0.3) * 0.3 * pb(0.5) * 0.3
0.7 * pa(0.3) * 0.3 * pb(-0.1) * 0.2 * pc(0.3) * 0.5 * pc(0.5) * 0.4


"""
A 0.7  N(0,  1.8^2)
B 0.3  N(-1, 1.6^2)
C 0    N(1,  1.3^2)
E 0    

    A       B       C       E
A   0.5     0.3     0.1     0.1 
B   0       0.5     0.2     0.3
C   0       0.1     0.5     0.4
"""
