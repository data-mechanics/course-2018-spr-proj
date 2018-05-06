import { Component, OnInit } from '@angular/core';
import { ApiService } from '../api.service';

declare var google: any;

@Component({
  selector: 'app-shouldimove',
  templateUrl: './shouldimove.component.html',
  styleUrls: ['./shouldimove.component.css']
})

export class ShouldimoveComponent implements OnInit {

  constructor(private api: ApiService) { }

  center;
  map;
  markers = [];
  loading = null;
  current;


  ngOnInit() {
    this.center = {lat: 42.3501, lng: -71.0589};
    this.map = new google.maps.Map(document.getElementById('map'),
    {zoom: 11.5,
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
      this.map.setZoom(11.5);
    });

    this.map.controls[google.maps.ControlPosition.TOP_RIGHT].push(controlDiv);

  }

  addArrows() {
    const controlDiv = document.createElement('div');
    const controlUI = document.createElement('div');
    controlUI.style.backgroundColor = '#fff';
    controlUI.style.border = '2px solid #fff';
    controlUI.style.borderRadius = '3px 0 0 3px';
    controlUI.style.boxShadow = '0 2px 6px rgba(0,0,0,.3)';
    controlUI.style.cursor = 'pointer';
    controlUI.style.marginBottom = '22px';
    controlUI.style.textAlign = 'center';
    controlUI.title = 'View Next Marker';
    controlUI.innerText = '<';
    controlUI.style.padding = '6px';
    controlUI.style.display = 'inline-block';
    controlDiv.appendChild(controlUI);

    const controlUI2 = document.createElement('div');
    controlUI2.style.backgroundColor = '#fff';
    controlUI2.style.border = '2px solid #fff';
    controlUI2.style.borderRadius = '0 3px 3px 0';
    controlUI2.style.boxShadow = '0 2px 6px rgba(0,0,0,.3)';
    controlUI2.style.cursor = 'pointer';
    controlUI2.style.marginBottom = '22px';
    controlUI2.style.textAlign = 'center';
    controlUI2.title = 'View Previous Marker';
    controlUI2.innerText = '>';
    controlUI2.style.padding = '6px';
    controlUI2.style.display = 'inline-block';
    controlDiv.appendChild(controlUI2);

    controlUI.addEventListener('click', () => {
      this.cycleMarkersLeft();
    });

    controlUI2.addEventListener('click', () => {
      this.cycleMarkersRight();
    });

    this.map.controls[google.maps.ControlPosition.BOTTOM_LEFT].push(controlDiv);
  }

  focusOnMarker(marker) {
    this.map.setCenter({lat: marker.position.lat(), lng: marker.position.lng()});//marker.position;
    this.map.setZoom(15);
  }

  cycleMarkersLeft() {
    if (this.current !== undefined) {
      this.current = this.current - 1;
      if (this.current < 0) {
        this.current = this.markers.length - 1;
      }
    } else {
      this.current = 0;
    }
    this.focusOnMarker(this.markers[this.current]);
  }


  cycleMarkersRight() {
    if (this.current !== undefined) {
      this.current = this.current + 1;
      if (this.current >= this.markers.length) {
        this.current = 0;
      }
    } else {
      this.current = 0;
    }
    this.focusOnMarker(this.markers[this.current]);
  }

  clearMarkers() {
    for (let i of this.markers) {
      i.setMap(null);
    }
    this.markers = [];
  }

  addMarker(latlng) {
    if (this.loading == null) {
      const marker = new google.maps.Marker({
        position: latlng,
        map: this.map
      });
      this.markers.push(marker);
    }
  }

  runkmeans(form) {
    this.clearMarkers();
    this.api.findCentroids(form['number']).subscribe(data => {
      for (let i of data) {
        this.addMarker(i);
      }
      this.addArrows();
    });
  }

}
