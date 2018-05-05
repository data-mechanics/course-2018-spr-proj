
var rNames = document.getElementById("restaurantNames");
var lat,lon;
var map = document.getElementById("map");
var layerId = 0;
var retryCnt = 0;

var userMarker = null;
var useCurrentLocClicked = false;

//global var layer group
var userLayerGroup = L.layerGroup();
var restaurantLayerGroup = L.layerGroup();

//new icon specs to differ from user location
var greenIcon = new L.Icon({
  iconUrl: 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-green.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41]
});

requestPermission();

function requestPermission() {
  navigator.permissions.query({name:'geolocation'}).then(function(result) {
    if (result.state === 'granted') {
      console.log("granted");
      getLocation();
    } else if (result.state === 'prompt') {
      console.log("prompt");
      getLocation();
    }
    // Don't do anything if the permission was denied.
  });
}

function getLocation() {
  console.log("getLocation() called")
  let id, options;
  useCurrentLocClicked = true;

  function error(err) {
    retryCnt += 1;
    console.warn('ERROR(' + err.code + '): ' + err.message + ', num retries=' + retryCnt);
    getLocation();
  }

  options = {
    enableHighAccuracy: true, // if possible, higher accuracy but slower response times
    timeout: 5000
  };

  id = navigator.geolocation.getCurrentPosition(sendPosition, error, options);
}

function sendPosition(position) {
  console.log("sendPosition() called")
  retryCnt = 0;

  // TODO: send location to backend?

  // show location on client if the button for getting current location was clicked last
  if(useCurrentLocClicked) {
    updateLatLonVars(position.coords.latitude,position.coords.longitude);
    showCurrPosition();
    enableSubmitButton();
  }
}

function updateLatLonVars(latitude,longitude) {
  lat = latitude;
  lon = longitude;
}

function showCurrPosition() {
  loc.innerHTML = "Latitude: " + lat
                    + "<br>Longitude: " + lon;
  map.flyTo([lat, lon], 12);

  //check if markers already exist on map
  if(map.hasLayer(userLayerGroup)) {
      map.removeLayer(userLayerGroup);
      userLayerGroup.clearLayers();
  }

  userMarker = L.marker([lat, lon]).addTo(map);
  userLayerGroup.addLayer(userMarker);
  userLayerGroup.addTo(map);
}

function enableSubmitButton() {
  console.log("enableButton");
  document.getElementById("submit_button").disabled = false;
}


function submit() {
  let yelpscale = document.getElementById("yelp_scale").value;
  let violationscale = document.getElementById("violation_scale").value;
  let num = document.getElementById("num_restaurant").value;
  //console.log(yelpscale,violationscale,lat,lon,num);
  $.get("/receivedata", {"yelp": yelpscale,
    "violation": violationscale,
    "lat": lat,
    "lon": lon,
    "rest":num
  }).done(function(data){
    var t = JSON.parse(data);

    //check if markers already exist on map
    if(map.hasLayer(restaurantLayerGroup)) {
        map.removeLayer(restaurantLayerGroup);
        restaurantLayerGroup.clearLayers();
    }

    var x;
    //adding marker to restaurantLayerGroup
    for (x in t['restaurants']) {
      console.log(t['restaurants'][x]);
      let latInfo = t['restaurants'][x][1];
      let longInfo = t['restaurants'][x][2];
      let restaurantInfo = t['restaurants'][x][0]+"<dd>"+"Health Violation Score: " + t['restaurants'][x][3]
      + "<dd>"+ "Yelp Rating: "+ t['restaurants'][x][4];
      var tempMarker = L.marker([latInfo, longInfo], {icon: greenIcon}).bindPopup(restaurantInfo);
      restaurantLayerGroup.addLayer(tempMarker);
    }
    //add layer group to map
    restaurantLayerGroup.addTo(map);
  });

}

function addUserMarker(e) {
  enableSubmitButton();           // make sure to enable submit button in case browser doesn't get current location prior to this
  useCurrentLocClicked = false;   // if getLocation is still waiting for browser, this prevents the location updating to the user's location
  updateLatLonVars(e.latlng.lat,e.latlng.lng);
  showCurrPosition();
}

function makeMap() {
  map = L.map('map', {fadeAnimation: true}).setView([42.3601, -71.0589], 11);
  var layer = L.tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
  });
  // Now add the layer onto the map
  map.addLayer(layer);

  // on click listener
  map.on('click', addUserMarker);
}
