import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';

import { Streaming } from './streaming/streaming';
import { Sidebar } from './sidebar/sidebar';
import { StreamPage } from './stream-page/stream-page';
import { Shell } from './shell/shell';
@Component({
  selector: 'app-root',
  imports: [Shell],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  protected readonly activeComponent = signal<'stream-page' | 'streaming'>('stream-page');

  protected setActive(component: 'stream-page' | 'streaming'): void {
    this.activeComponent.set(component);
  }
}
