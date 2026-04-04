import { Component } from '@angular/core';
import { Titlebar } from '../titlebar/titlebar';
import { Sidebar } from '../sidebar/sidebar';
import { RightSidebar } from '../right-sidebar/right-sidebar';
import { RouterOutlet } from '@angular/router';

@Component({
  selector: 'app-shell',
  imports: [Titlebar, Sidebar, RightSidebar, RouterOutlet],
  templateUrl: './shell.html',
  styleUrl: './shell.css',
})
export class Shell {}
