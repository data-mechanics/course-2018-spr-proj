### Colin Stuart: Project 2


Food deserts are areas are areas with limited access to affordable and nutritious food. The Massachussetts Public Health 
Association has found that nearly 40% of Massachussetts residents live these areas of limited grocery access. For my 
project, I propose a neighborhood-by-neighborhood analysis of food availability in Boston.
I plan to analyze datasets containing the geographic coordinates of each area code and neighborhood within Boston's limits,
to better understand which parts of the city are most affected. I will use data from the Google Places API to create a dataset
of grocery stores within the city. In addition, I plan to use the poverty rate data from Yash's group to better identify the 
percentage of Boston residents in poverty in each neighborhood. Future analysis could include data on public transportation 
in Boston and food licenses to better understand accessibility and other healthy food options. 

1. **Optimization Technique**

    I chose to use k-means to identify where grocery stores are clustered within Boston, and to better understand what grocery
    stores are closest to all other grocery stores. Identifying these clusters of grocery stores allows us to identify areas
    where the Boston community has easy access to healthy and fresh food options.
    	
2. **Statistical Analysis**
    
    I compared the number of grocery stores in each Boston neighborhood with the percentage of Boston's impoverished living 
    in that neighborhood, in order to better understand if there was any correlation between low-income neighborhoods
    and a dearth of grocery stores in that area. I found a positive correlation coefficient, indicating a positive linear relationship between the number 
    of grocery stores and the percent of Boston’s impoverished living in a given neighborhood. However, this 
    relationship could simply be because neighborhoods with greater populations will have a greater percent of Boston’s 
    impoverished and more grocery stores. Additionally, the p-value was around 0.005. 
    
3. **Limitations**
	
    Simply locating grocery stores geographically does not fully indicate which areas in Boston are food deserts. 
    Future work on this project could include identifying grocery stores that are most and least accessible by public transportation and
    comparing average food prices across different grocery store chains to understand which chains are most accessible to low-income
    residents.