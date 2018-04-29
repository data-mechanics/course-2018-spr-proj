import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ShouldimoveComponent } from './shouldimove.component';

describe('ShouldimoveComponent', () => {
  let component: ShouldimoveComponent;
  let fixture: ComponentFixture<ShouldimoveComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ShouldimoveComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ShouldimoveComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
