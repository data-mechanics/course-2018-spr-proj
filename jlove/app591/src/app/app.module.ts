import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from './api.service';
import {HttpClientModule} from '@angular/common/http';


import { AppComponent } from './app.component';
import { ShouldimoveComponent } from './shouldimove/shouldimove.component';


@NgModule({
  declarations: [
    AppComponent,
    ShouldimoveComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpClientModule
  ],
  providers: [ApiService],
  bootstrap: [AppComponent]
})
export class AppModule { }
