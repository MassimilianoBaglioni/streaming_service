import { ComponentFixture, TestBed } from '@angular/core/testing';

import * as tauriInvoke from '../utils/tauri-invoke';
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

  it('should clear the invite ticket when starting streaming fails', async () => {
    component.connectionMode = 'invite';
    component.inviteTicket = 'stale-ticket';

    spyOn(tauriInvoke, 'callCommand').and.rejectWith(new Error('boom'));

    await component.startStreaming();

    expect(component.inviteTicket).toBeNull();
    expect(component.canStartStream).toBeFalse();
  });
});
