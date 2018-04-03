# CS591L1 Project 2 

### Team Member 

Xinchun He\
Shizhan Qi\
Dayuan Wang\
Teng Xu 

### Project Purpose 

In Project 1, we find the shortest distance between 
a school and a subway station. In this project we 
are using the data to find the new optimized placements 
for bike hubs.
 

### Project Justification

First we use the data from Project 1 to find the average 
and the longest distance of all schools to its closest 
subway station. For all schools that its distance to the 
closest subway stations that are between the average and 
the longest distance, we define that these schools need  
bike hubs. For each school that needs the bike hub, we 
find all subway station that is under the 
longest distance(by using modified K mean algorithm). We will create a system for z3 solver. 
The z3 solves and finds the optimized placement for the 
least number of bike hubs. After that, we compute the 
distance of all schools to its closest subway station
(if the school does not need a bike station) or bike hub 
(if the school need a bike hub). Then we compute the 
correlation between the new bike hub placement and the 
hubway station placement from Project 1. 

### How to run 

Using trial mode saves a lot of time 
```
python execute.py debhe_shizhan0_wangdayu_xt --trial
```
