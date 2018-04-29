import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';


import { AppComponent } from './app.component';
import { ShouldimoveComponent } from './shouldimove/shouldimove.component';


@NgModule({
  declarations: [
    AppComponent,
    ShouldimoveComponent
  ],
  imports: [
    BrowserModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
