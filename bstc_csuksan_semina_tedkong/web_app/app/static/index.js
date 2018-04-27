
var rNames = document.getElementById("restaurantNames");
var lat,lon;
var map = document.getElementById("map");
var layerId = 0;
var retryCnt = 0;
//global var layer group
var layerGroup = L.layerGroup()
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
  var id, target, options;

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

  // show location on client
  updateLatLonVars(position);
  showPosition(position);
  enableButton();
}

function updateLatLonVars(position) {
  lat = position.coords.latitude;
  lon = position.coords.longitude;
}

function showPosition(position) {
  loc.innerHTML = "Latitude: " + lat
                    + "<br>Longitude: " + lon;
  map.flyTo([position.coords.latitude, position.coords.longitude], 12);
  L.marker([position.coords.latitude, position.coords.longitude]).addTo(map);
}

function enableButton() {
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
      if(map.hasLayer(layerGroup)) {
          map.removeLayer(layerGroup);
          layerGroup.clearLayers();
        } 
      //var x as iterator
      var x;
      //adding marker to layerGroup
      for (x in t['restaurants']) {
        console.log(t['restaurants'][x]);
        let latInfo = t['restaurants'][x][1];
        let longInfo = t['restaurants'][x][2];
        let restaurantInfo = t['restaurants'][x][0]+"<dd>"+"Health Violation Score: " + t['restaurants'][x][3] 
        + "<dd>"+ "Yelp Rating: "+ t['restaurants'][x][4];
        var tempMarker = L.marker([latInfo, longInfo], {icon: greenIcon}).bindPopup(restaurantInfo);
        layerGroup.addLayer(tempMarker);
      }
      //add layer group to map
      layerGroup.addTo(map);
      });
    
}
