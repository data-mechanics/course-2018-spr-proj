
from math import sin, cos, sqrt, atan2, radians

def latLongDist(p, q):
        p = (radians(p[0]), radians(p[1]))
        q = (radians(q[0]), radians(q[1]))
        dlon = q[1] - p[1]
        dlat = q[0] - p[0]

        a = sin(dlat / 2)**2 + cos(p[0]) * cos(q[0]) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return 6373 * c

print(latLongDist((42.349617, -71.099541), (42.351805, -71.116493)))