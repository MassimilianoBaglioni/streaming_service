import { Routes } from '@angular/router';

import { StreamPage } from './stream-page/stream-page';
import { VideoSettingsPage } from './video-settings-page/video-settings-page';

export const routes: Routes = [
  { path: '', component: StreamPage },
  { path: 'stream', component: StreamPage },
  { path: 'video-settings', component: VideoSettingsPage },
];
