## CS591-L1 Data Mechanics Term Project: <br/>Analysis on Crash Accidents at Night Time
Team members: Li Wang | Po-Yu Hsieh
***
### Introduction
***
According to national data, 49% of fatal crashes happen at night, with many factors that is controllable or not controllable. In this project, we are trying to define two problems and provide possible solutions for them. The first is to find the relationship between car crash accidents , and street furnitures such as street lights and traffic signals nearby, to further explore how these settings can play a role in crash accident prevention. The second problem lives on the fact that some accidents cannot be prevented. Under such situations, the reaction time of nearby hospitals and police stations would be crucial. So we want to find out recommended places for new hospitals or police stations which can shorten the time to position where an car accident happens.

### Dataset
***
Belows are dataset we used for our analysis:
1. Traffic Signals in Boston. [Link](http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0)
<br/>Contains information of 839 traffic signals in Boston.
2. Street Lights in Boston. [Link]( https://data.boston.gov/dataset/streetlight-locations/resource/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5)
<br/>Contains information of 74,065 street lights in Boston
3. *Hospitals in Boston.* [Link]( https://data.boston.gov/dataset/hospital-locations/resource/6222085d-ee88-45c6-ae40-0c7464620d64)
<br/>Contains information of 26 hospitals in Boston.
4. *Police Stations in Boston.* [Link]( https://data.boston.gov/dataset/boston-police-stations/resource/0b2be5cb-89c6-4328-93be-c54ba723f8db)
<br/>Contains information of 13 police stations in Boston.
5. *Crash accidents during 2015, Boston.* [Link]( http://datamechanics.io/data/liwang_pyhsieh/crash_2015.json)
<br/>This dataset is retrieved from MassDot Portal, which contains 4,110 tuples of crash accident details.
Source: MassDot Crash Portal. [Link](https://services.massdot.state.ma.us/crashportal/)

### Methodology
***
#### Data Transformation
Since we are interested in crash cases during night time, first we select cases that occured during PM 6:00 to AM 6:00 from the crash dataset. We also convert coordinate attributes from EPSG:26986 (coordination system for Massachusetts area) system to world map system for consistency in further analysis. Also we compute the number of other accidents within 3 kilometer radius and call it 'accident frequency', which shows how often do crash accidents happen in the neighborhood.

To find out the nearest facilities at each accident position, we compute the distance (by sphere distance) to the nearest hospital and poice station. For street light and signal dataset, we compute the number of surrounding street lights and traffic signals within 1 kilometer radius for each crash position. The work on range search and nearest-neighbor search are aided by using R-tree index.

We also conduct K-means algorithm in order to find out accident distribution and 'hot spots' where accidents happened more frequently. Given the number of clusters, K-means algorithm tries to minimize average distance from input points to the nearest cluster center. From this perspect we can deem the a mean point (cluster center) as the center of area with relatively higher accident density. In our analysis, we cluster crash accidents by location into 5 groups.

Finally, from the above result we computes average distance of nearest hospital distance and police station for each cluster. We think the higher the average distance a cluster is, the overall time required to arrive the point for rescue is higher.

#### Model Constraint Satisfaction Solving
In this part, we want to find out possible place for building new hospitals, and these locations should decrease by given rate. In our experiment we hope to reduce average distance to the nearest hospital for crash accidents by 20 percentes. Note that this is a constraint satisfaction problem, but not an optimization problem because we doesn't provide a metric.

#### Statistical Analysis
Furthermore, we are interested in whether a place with fewer lighting or signals will cause higher rate of crash accident. We compute correlation-coefficient and pearson-p value between observations of accident density, number of surrounding street lights, and number of surrounding traffic signals, which are generated from previous data transformation step.

### Result
***
Most of the results from previous analysis are combined into visualization part.
#### Statistical Analysis
|*Corr-Coef/p-val*|Accident Density|Surrounding Lights|Surrounding Signals|
|---|---|---|---|
|Accident Density||0.2265 / 3.036e-18|-0.12834 / 1e-5|
|Surrounding Lights|||0.0097 / 0.7126|

From the result we can conclude that there exist positive relationship between accident density and number of surrounding lights, while negative relationship between accident density and number of surrounding traffic signals. However, from our observation to the dataset, in most cases there's no street light or signal near to the crash position, so further and detailed analysis still required for finding out the actual relationship between these facilities and crash frequency.

#### Visualization
In figure 1, we plotted the crash accident positions and result of K-means algorithm return, which show a optimal location for the facilities. The red markers on the graph shows optimal five points obtained from K-mean algorithm, and the red circles show coverage area of clusters. We also use different color on dots to show which cluster a crash position belongs to. From the clusters we can see some areas where accident occurences are denser.
![Figure 1: Cluster Result](https://github.com/pykenny/course-2018-spr-proj/blob/master/liwang_pyhsieh/img/Snapshot_Clustering.png?raw=true)
Figure 1: Visualization of clustering distribution and range of crash positions

In figure 2, we plotted the accident density on a heat map around the Boston area as the background. Position with warm color shows where most crash accidents happen between time 6 PM - 6 AM. The red markers on the map represent the location of hospitals and the blue markers represent the location of police stations. The green markers on the map show recommended places for new hospitals from our constraint-satisfaction solver. We can see these positions are near to hot spots in the heat map. 
![Figure 2: Heatmap](https://github.com/pykenny/course-2018-spr-proj/blob/master/liwang_pyhsieh/img/Snapshot_Heatmap_Facilities.png?raw=true)
Figure 2: Visualization of crash heat map and positions of hospitals (red points) and police stations (blue points). Green points shows the recommended positions for new hospitals.
### Conclusion and Future Work
The anlysis not only gives us an overview of crash accident distribution during night time, but also shows relationship between accident frequency and surrounding factors such as lighting or signals. In addition, it comes up with some possible suggestions for solution to reduce response time for accident rescue.

However, we simplify many factors such as using direct distance between crash accidents, facilities, and road furnitures, but in the real world situation, the path should be computed based on roadmap information and traffic status to reflect actual responding time to an accident point. Lights and signals might not be accessible even if the count for surrounding area is relatively high. Few traffic signals might be on the road where an accident occurs, and many signals on another road nearby. These are some directions in what we can do further for the future work.

### Instructions about the Code
***
#### Main Program
The programs are developed under Python 3.6.
To run the main program, please run this command under `course-2018-spr-proj` directory:

    python3.6 execute.py liwang_pyhsieh
For fast testing, you can also run it under trial mode, which through the whole process rapidly just using a small portion of our dataset:

    python3.6 execute.py liwang_pyhsieh --trial
#### Visualization
Code for visualization are placed under `liwang_pyhsieh/visualization` directory.

`Visualization_Clustering.py` can generate visualization result in Figure 1. It will save the result to file `Map_Clustering.html` under the same directory where the code is executed. 

`Visualization_Facilities.py` can generate visualization result in Figure 2. It will save the result to file `Map_Hospitals_PoliceStations.html` under the same directory where the code is executed.

`Visualization_FacilityCount.ipynb`
Although result from this code is not integrated in this report, this still helps you get to know how to use our data. It generates histogram from transformed data in our database. You need to install `Jupyter` to open the notebook file.

#### Library Dependencies
These are Python libraries required to run our code, other than python standard libraries:
    
    dml        # Main library for projects in CS591-L1
    prov       # A library helps generate data provenance information
    pymongo    # Python API for mongodb operations
    numpy      # Array and matrix operation
    pyproj     # Cartographic and geodetic computation
    geopy      # Geocoding toolkit
    rtree      # R-Tree indexing for miltidimensional search
    sklearn    # scikit-learn, a library for machine learning applications
    scipy      # Scientific computation
    pandas     # Library for data analysis
    matplotlib # 2-D graph plotting
    seaborn    # Styled theme for matplotlib
   

