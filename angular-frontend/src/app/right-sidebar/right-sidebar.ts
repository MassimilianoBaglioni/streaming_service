import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-right-sidebar',
  imports: [CommonModule],
  templateUrl: './right-sidebar.html',
  styleUrl: './right-sidebar.css',
})
export class RightSidebar {
  streamActive = false;
  bitrate = '—';
  duration = '—';
  viewers = 0;
  resolution = 'Not set';
  fps = '—';
}
