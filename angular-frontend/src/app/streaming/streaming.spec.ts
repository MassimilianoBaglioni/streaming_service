import { ComponentFixture, TestBed } from '@angular/core/testing';

import { Streaming } from './streaming';

describe('Streaming', () => {
  let component: Streaming;
  let fixture: ComponentFixture<Streaming>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [Streaming]
    })
    .compileComponents();

    fixture = TestBed.createComponent(Streaming);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
