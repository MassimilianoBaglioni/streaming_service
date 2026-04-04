import { ComponentFixture, TestBed } from '@angular/core/testing';

import { StreamPage } from './stream-page';

describe('StreamPage', () => {
  let component: StreamPage;
  let fixture: ComponentFixture<StreamPage>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [StreamPage]
    })
    .compileComponents();

    fixture = TestBed.createComponent(StreamPage);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
