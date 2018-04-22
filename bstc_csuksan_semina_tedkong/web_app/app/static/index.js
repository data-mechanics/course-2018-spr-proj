
var rNames = document.getElementById("restaurantNames");
var loc = document.getElementById("loc"); // must be named loc or else page refreshes infinitely
var msg = document.getElementById("userMsg");

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
  showPosition(position);
}

function showPosition(position) {
  msg.innerHTML = "Your current location... "
  loc.innerHTML = "Latitude: " + position.coords.latitude
                    + "<br>Longitude: " + position.coords.longitude;
}
