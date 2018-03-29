import z3

(x1,x2,x3,x4,x5,x6,x7) = [z3.Real('x'+str(i)) for i in range(1,8)]

S = z3.Solver()

# Only allow non-negative flows.
for x in (x1,x2,x3,x4,x5,x6,x7):
    S.add(x >= 0)
    
# Edge capacity constraints.
S.add(x2 <= 7, x3 <= 8, x4 <= 6)
S.add(x5 <= 3, x6 <= 4, x7 <= 5)

# Constraints derived from graph topology.
S.add(x1 == x2+x3, x2 == x4+x5, x3+x4 == x6, x5+x6 == x7)

S.add(x1 > 0) # We want a positive flow.

print(S.check())
print(S.model())