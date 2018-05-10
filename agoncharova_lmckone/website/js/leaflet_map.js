(function() {
  var map = L.map('map').setView([42.36, -71.03], 11);
  L.tileLayer('https://stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}.{ext}', {
    attribution: 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data &copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    subdomains: 'abcd',
    minZoom: 0,
    maxZoom: 20,
    ext: 'png'
  }).addTo(map);


  function getColor(d) {
    return d > 1000 ? '#741b47' :
      d > 2.5 ? '#842b56' :
      d > 2 ? '#953f68' :
      d > 1.7 ? '#a6557b' :
      d > 1.4 ? '#b76f91' :
      d > 1 ? '#c88da8' :
      d > 0.5 ? '#d9adc1' :
      '#ead1dc';
  }

  function style(feature) {
    return {
      fillColor: getColor(feature.properties.score),
      weight: 0.5,
      opacity: 1,
      color: 'white',
      fillOpacity: 0.7
    };
  }
  L.geoJson(tracts, { style: style }).addTo(map);
})();