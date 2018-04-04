# CS591L1 Project 2 

### Team Member 

Xinchun He\
Shizhan Qi\
Dayuan Wang\
Teng Xu 

### Project Purpose 

In the last project, we have analyzed how close the hubway 
station and the subway station are to each of the schools. For each school, 
we found out its cloest subway station and hubway station, as well as the distances between them. 
In this project, we try to find the best initial placement for the ofo bikes so that the bikes would be utilized the most. 
While there are many factors that could be taken into account, for this project, 
we are just targeting the schools to let each school have a more convinient way to get to the subway station. 
We are going to use K mean algorthm to preprocess the data and use the
z3 solver to help us to find the approprate assignment(for the location to
place the new bike hubs). After that, we are going to get the data about
the new assignment and compare it with the existing hubway station location
to find their coefficient and covariance (to see if the two different assignments
have any similarity. And try to see which one is better.)
 

### Constraint Optimization

The following part is inplemented in the file optimizedBikePlacement.py and the ofo bike placement assignment is 
stored in the data collection debhe_shizhan0_wangdayu_xt.optimizeBikePlacement.

We are going to do following steps in this part: 
1. Find the list schools that are "far away" to its closest subway station. If the school is very close to the subway
station, people can just walk there. (i.e. no bikes need to be placed at the station)
2. For each of the school, there is a list of subway stations that are being considered (those within a certain range of distance away
from the school). These subway stations are our potential location to place the bike hubs.(by using modified K mean algorithm)  
3. Set the constraint for the z3 solver as the following: 
For each of the target school, there must exist a subways station(in a certain range to that school) 
get assigned for a bike hub. Note that a assignment of placing ofo bikes at a subway station could potentially fit the requirement
for multiple schools. 
4. We are going to use the z3 solver to test if there is a satisfaction assignment for all the school. 
5. Once we find it is possible to do, we are going to use the z3 solver to optimized the total number of ofo bike placement locations.
(In our case, we consider to find the minimum numbers of bike hubs that can fit the constraint for all the schools.)
In our code, we used the binary bit adding way of finding the minimun number that can make the whole system satisfy, which
can cut the runtime by a huge fraction. By doing this, we are able to run this with the whole data in less than 5 minutes.
6. Then we are able to get the optimized assignment(minimum number of bike hub) that can fit the requirement for all 
the schools.

### Statistics
This part is implemented in two files. newSchoolSubDis.py and statistics.py\
The file statistic result is in the data collection debhe_shizhan0_wangdayu_xt.statistics\
Once we get the assignment, we are going to do the following: 
1. The z3 solver will give us the list of optimized (minimum number) assignment of bike hubs based on the
location of the subway station. We are going to find the list of subway stations that get assigned.  
2. Then for those schools that is "far away" to its closest subway station, we are going to find the closest new bike 
hub (also subway station since we place bike hubs on subway station) and get their locations. 
3. Then we are going to use this new data and the old hubway station's data to compute the correlation coefficient and covarience. 

### Project justification
In our project, we are trying to give the students in the school a better choice of transportation. Our way to set up the z3 problem
system can make sure that for all the school that is far from its closest subway station there must exist a new bike hub around it 
in a small range. By the statistics, it shows that our new data gets a lot different with the existing hubway stations. On 
the other hand, it shows us that our way of placing is totally different from the existing one. It might not be the best
algorithm for now, but this way of setting up should work if we can optimize our algorithm a bit. 

### How to run 

```
python execute.py debhe_shizhan0_wangdayu_xt 
```

Running trial mode by
```
python execute.py debhe_shizhan0_wangdayu_xt --trial
```
