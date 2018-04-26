
var rNames = document.getElementById("restaurantNames");
var lat,lon;
var map = document.getElementById("map");

var retryCnt = 0;



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
    // console.log(yelpscale,violationscale,lat,lon,num);
    $.get("/receivedata", {"yelp": yelpscale, 
      "violation": violationscale,
      "lat": lat,
      "lon": lon,
      "rest":num
    }).done(function(data){console.log(data)});
    // , function (res) {
    //   updateMarkers
    // })
}
