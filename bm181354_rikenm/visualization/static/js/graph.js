queue()
    .defer(d3.json, "/hubway/projects")  // <--- route that contains all my json datas
    .defer(d3.json,"/hubway/boston")
    .defer(d3.json,"/cluster/1")
    .defer(d3.json,"/hubway/pop")
    .await(makeGraphs);

var i = 0.0001;
var flag = true;
var counter = 0;

    function makeGraphs(error, projectsJson,bostonJson,clusterJson,popularityJson) {
    
        var coor = [-71.157609+i,42.358888+i];
        i = i + 0.00001; 

        console.log(clusterJson)  

        console.log(popularityJson[clusterJson][0])

        var set_data = [[-71.09482660737189,42.362429532712525],
        [-71.10144447293223,42.34177428658286 ]]


        // this will be replaced by clusterJson
        // TODO : Replace this with original data 


        var popularity = [1272.1538461538462,829.3,583.0,2146.6666666666665,1886.1538461538462,1084.3,557.2,4711.5,2769.5833333333335,2786.75,89.0,1918.75,5042.764705882353,1163.0,3477.5714285714284,2.0, 23.0,1707.3333333333333, 2866.8571428571427,440.0]

        var data1   = [ 

            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][1], 'title': 'Cluster 1'  },
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][2], 'title': 'Cluster 2'  },
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][3], 'title': 'Cluster 3'  },
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][4], 'title': 'Cluster 4'  },
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][5], 'title': 'Cluster 5'  },
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][6], 'title': 'Cluster 6'  },
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][7], 'title': 'Cluster 7'  },
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][8], 'title': 'Cluster 8'  },
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][9], 'title': 'Cluster 9'  },
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][10], 'title': 'Cluster 10'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][11], 'title': 'Cluster 11'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][12], 'title': 'Cluster 12'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][13], 'title': 'Cluster 13'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][14], 'title': 'Cluster 14'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][15], 'title': 'Cluster 15'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][16], 'title': 'Cluster 16'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][17], 'title': 'Cluster 17'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][18], 'title': 'Cluster 18'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][19], 'title': 'Cluster 19'  }, 
            { 'Expt': 1, 'Run': 1, 'popularity': popularityJson[clusterJson][0], 'title': 'Cluster 20'  }
            
          ];


       

        var ls = [[ -71.07729, 42.36881 ],[ -71.03689, 42.39571 ]];
//
        //

        
        counter = (counter + 1);
        
        // change this 
      
            //Clean projectsJson data
        var hubwayProjects = projectsJson;
        
        var dx = crossfilter(data1)   // clusters
        var ndx = crossfilter(hubwayProjects);
    
        //Define Dimensions
        var latDim = ndx.dimension(function(d) { return d["Latitude_normalized"]; });
        var longDim = ndx.dimension(function(d) { return d["Longitude_normalized"]; });

        var popularityLevelDim = dx.dimension(function(d) { return d.title; });

        var yDim = ndx.dimension(function(d) { return d["Y_label"]; });
        //var totalDonationsDim  = ndx.dimension(function(d) { return d["Y_label"]; });

        var all = dx.groupAll();

        var lat_longChart = dc.barChart("#chart");
        
        var chart = dc.pieChart("#Y_label");
        //var bostonChart = dc.geoChoroplethChart("#Boston-chart");
        var numberND = dc.numberDisplay("#number");
        var totalND = dc.numberDisplay("#total");

        var y_labHistChart  = dc.barChart("#hist");


       latGroup = latDim.group()
       longGroup = longDim.group()
       popGroup = popularityLevelDim.group().reduceSum(function(d) { 
                return d.popularity;
      });

       yGroup = yDim.group().reduceCount();


       y_labHistChart
        .width(500)
        .height(264)
        .dimension(yDim)
        .group(yGroup)
        .x(d3.scale.linear().domain([0,18]))
        .elasticY(true)
        .colors(["#5200cc"])
        .render();


       numberND
        .formatNumber(d3.format("d"))
		.valueAccessor(function(d){return 200 + clusterJson; })
		.group(all)
        .render();
        
        totalND
        .formatNumber(d3.format("d"))
		.valueAccessor(function(d){return clusterJson; })
		.group(all)
        .render();

        chart
        .width(500)
        .height(550)
        .dimension(popularityLevelDim)
        .group(popGroup)
        .legend(dc.legend())
        .innerRadius(80)
        .colors(["#a366ff","#b380ff","#c299ff","#d1b3ff","#e0ccff","#f0e6ff","#ffffff","#6600ff","#5200cc","#cc99ff","#751aff","#8533ff","#944dff"])
        .render();

        lat_longChart
		.width(600)
		.height(160)
		.margins({top: 10, right: 50, bottom: 30, left: 50})
		.dimension(latDim)
		.group(longDim.group())
		.transitionDuration(500)
		.x(d3.time.scale().domain([-0, 4]))
		.elasticY(true)
		.xAxisLabel("Year")
        .yAxis().ticks(4);

        ////////

        
        // Width and Height of the whole visualization
    var width = 670;
    var height = 645;


    var main = d3.select("main");
    // Create SVG
    main.select( "map" ).selectAll("svg").remove();
    var svg = main.select( "map" )
    .append( "svg" )
    .attr( "width", width )
    .attr( "height", height );


// Append empty placeholder g element to the SVG
// g will contain geometry elements
    var g = svg.append( "g" );

    // projection
    var albersProjection = d3.geo.albers()
    .scale( 190000 )
    .rotate( [71.057,0] )
    .center( [0, 42.313] )
    .translate( [width/2,height/2] );

// Create GeoPath function that uses built-in D3 functionality to turn
// lat/lon coordinates into screen coordinates
    
    // path 
    var geoPath = d3.geo.path()
    .projection(albersProjection);

   
// Classic D3... Select non-existent elements, bind the data, append the elements, and apply attributes
 var j;
  for(j=0;j<26;j++){
    g.selectAll( "path" )
    .data(bostonJson.features)  
    .enter()
    .append( "path" )
    .attr( "fill", "#cce0ff") // change this color 
    .attr( "stroke", "#f2f2f2")
    .attr( "d", geoPath );
  }   

   // redraw 
    var svg = d3.select("main").select("map").select("svg");

    var color  =["#751aff","#8533ff","#944dff","#a366ff","#b380ff","#c299ff","#d1b3ff","#e0ccff","#f0e6ff","#ffffff","#6600ff","#5200cc","#cc99ff"]

    var color1 =  ["#4D567F",
		 "#a08f2f",
		 "#52ff7d",
         "#52ffa8",
         "#52ffd4",
		 "#52ffff",
		 "#52d4ff",
		 "#52a8ff",
		 "#527dff",
		 "#5252ff",
    	 "#7d52ff",
         "#d452ff",
         "#ff52a8",
		 "#ff527d",
         "#ff5252",
         "#990000",
         "#800000",
         "#660000",
		 "#ffffff",
         "#330000",
         "#1a0000"]


    // inserting the data
    var i;


    for(i = 0;i<19;i++){

    var points = svg.append("g")
        .selectAll( "path" )
        .data(data2[clusterJson]["old"][i].features) // data here 
        .enter()
        .append( "path" )
        .attr( "fill", color1[i])
        .attr( "stroke", "#fff" )
        .attr( "d", geoPath );
}


    // new point 
    var points = svg.append("g")
    .selectAll( "path" )
    .data(data2[clusterJson]["new"].features) // data here 
    .enter()
    .append("path")
    .attr("width",10)
    .attr("height",10)
    .attr( "fill", "#ffff1a" )
    .attr( "stroke", "#000" )
    .attr( "d", geoPath );

        ////////


    dc.renderAll();

    };

    function updateSlider(slideValue) {
        var sliderDiv = document.getElementById("sliderValue");

        sliderDiv.innerHTML = slideValue;

// call here
        queue()
            .defer(d3.json, "/hubway/projects")  // <--- route that contains all my json datas
            .defer(d3.json,"/hubway/boston")
            .defer(d3.json,"/cluster/"+slideValue)
            .defer(d3.json,"/hubway/pop")
            .await(makeGraphs);
    };
