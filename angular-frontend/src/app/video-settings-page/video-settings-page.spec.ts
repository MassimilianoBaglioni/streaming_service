import { ComponentFixture, TestBed } from '@angular/core/testing';

import { VideoSettingsPage } from './video-settings-page';

describe('VideoSettingsPage', () => {
  let component: VideoSettingsPage;
  let fixture: ComponentFixture<VideoSettingsPage>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [VideoSettingsPage]
    })
    .compileComponents();

    fixture = TestBed.createComponent(VideoSettingsPage);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
