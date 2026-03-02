import { Component, signal } from '@angular/core';
import { RouterOutlet } from '@angular/router';

import { Streaming } from './streaming/streaming';
@Component({
  selector: 'app-root',
  imports: [RouterOutlet, Streaming],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  protected readonly title = signal('angular-frontend');
}
