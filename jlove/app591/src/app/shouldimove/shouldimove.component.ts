import { Component, OnInit } from '@angular/core';

declare var google: any;

@Component({
  selector: 'app-shouldimove',
  templateUrl: './shouldimove.component.html',
  styleUrls: ['./shouldimove.component.css']
})

export class ShouldimoveComponent implements OnInit {

  constructor() { }

  center;
  map;
  marker;
  loading = null;


  ngOnInit() {
    this.center = {lat: 42.3501, lng: -71.0589};
    this.map = new google.maps.Map(document.getElementById('map'),
    {zoom: 13,
     center: this.center,
     streetViewControl: false,
     fullscreenControl: false,
     styles: [
            {elementType: 'geometry', stylers: [{color: '#FFFFFF'}]},
            {elementType: 'labels', stylers: [{visibility: 'off'}]},
            {
              featureType: 'poi.park',
              elementType: 'geometry',
              stylers: [{color: '#008080'}]
            },
            {
              featureType: 'road',
              elementType: 'geometry',
              stylers: [{color: '#D3D3D3'}]
            },
            {
              featureType: 'road',
              elementType: 'geometry.stroke',
              stylers: [{color: '#D3D3D3'}]
            },
            {
              featureType: 'road.highway',
              elementType: 'geometry',
              stylers: [{color: '#A9A9A9'}]
            },
            {
              featureType: 'road.highway',
              elementType: 'geometry.stroke',
              stylers: [{color: '#A9A9A9'}]
            },
            {
              featureType: 'transit',
              elementType: 'geometry',
              stylers: [{color: '#FFFFF'}]
            },
            {
              featureType: 'water',
              elementType: 'geometry',
              stylers: [{color: '#708090'}]
            },
          ]
     });

    const controlDiv = document.createElement('div');
    const controlUI = document.createElement('div');
    controlUI.style.backgroundColor = '#fff';
    controlUI.style.border = '2px solid #fff';
    controlUI.style.borderRadius = '3px';
    controlUI.style.boxShadow = '0 2px 6px rgba(0,0,0,.3)';
    controlUI.style.cursor = 'pointer';
    controlUI.style.marginBottom = '22px';
    controlUI.style.textAlign = 'center';
    controlUI.title = 'Reset Map';
    controlUI.innerText = 'Reset Map';
    controlUI.style.padding = '6px';
    controlUI.style.margin = '5px 5px';
    controlDiv.appendChild(controlUI);

    controlUI.addEventListener('click', () => {
      this.map.setCenter(this.center);
      this.map.setZoom(13);
    });

    this.map.controls[google.maps.ControlPosition.TOP_RIGHT].push(controlDiv);


    this.map.addListener('click', e => {
      this.addMarker(e.latLng);
    });
  }

  addMarker(latlng) {
    if (this.loading == null) {
      if (this.marker) {
        this.marker.setMap(null);
      }
      this.marker = new google.maps.Marker({
        position: latlng,
        map: this.map
      });
      this.map.zoom = 5;
      console.log(this.marker.position['lat']());
    }
  }

}
