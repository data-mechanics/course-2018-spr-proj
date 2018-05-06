import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class ApiService {

  constructor(private http: HttpClient) { }
  findCentroids(count: number): Observable<object[]>  {
    const httpParams = new HttpParams().set('number', count);
    return this.http.get<Object[]>('http://127.0.0.1:5000/kmeans', {params: httpParams});
  }
}
