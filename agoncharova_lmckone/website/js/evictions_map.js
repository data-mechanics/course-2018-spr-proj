(function() {
	console.log("evictions map ran")
  var inputValue = null;
  var years = [2013, 2014, 2015, 2016, 2017]
  // Width and Height of the whole visualization
  var width = 600;
  var height = 600;

  // Create SVG
  var svg = d3.select("#evictions_map")
    .append("svg")
    .attr("width", width)
    .attr("height", height);

  // Append empty placeholder g element to the SVG
  // g will contain geometry elements
  var g = svg.append("g");
  var albersProjection = d3.geoAlbers()
    .scale(170000)
    .rotate([71.057, 0])
    .center([0, 42.313])
    .translate([width / 2, height / 2]);
  var geoPath = d3.geoPath()
    .projection(albersProjection);
  g.selectAll("path")
    .data(neighborhoods.features)
    .enter()
    .append("path")
    .attr("fill", "#ccc")
    .attr("stroke", "#333")
    .attr("d", geoPath);

  var evictions = svg.append("g");

  function dateMatch(data, value) {
    var eviction_year = data.properties.year;
    if (inputValue == (2000 + eviction_year)) {
      if (inputValue == 2013) {
        return "DeepSkyBlue"
      }
      if (inputValue == 2014) {
        return "HotPink"
      }
      if (inputValue == 2015) {
        return "LightPink"
      }
      if (inputValue == 2017) {
        return "MediumSpringGreen"
      }
      this.parentElement.appendChild(this);
      return "red";
    } else {
      return "#999";
    };
  }

  function initialDate(data, i) {
    if ((2000 + data.properties.year) == 2013) {
      this.parentElement.appendChild(this);
      return "red";
    } else {
      return "#999";
    };
  }
  evictions.selectAll("path")
    .data(evictions_json.features)
    .enter()
    .append("path")
    .attr("fill", initialDate)
    .attr("stroke", "#999")
    .attr("stroke-width", "1px")
    .attr("class", "eviction")
    .attr("d", geoPath)
    .on("mouseover", function(d) {
      d3.select("h2").text(d.properties.address);
      d3.select(this).attr("class", "eviction hover");
    })
    .on("mouseout", function(d) {
      d3.select("h2").text("");
      d3.select(this).attr("class", "eviction");
    });

  d3.select("#timeslide").on("input", function() {
    update(+this.value);
  });
  // update the fill of each SVG of class "incident" with value
  function update(value) {
    document.getElementById("range").innerHTML = value;
    inputValue = value;
    d3.selectAll(".eviction")
      .attr("fill", dateMatch);
  }

})();