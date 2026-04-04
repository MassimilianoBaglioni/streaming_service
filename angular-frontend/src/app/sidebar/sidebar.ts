import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
@Component({
  selector: 'app-sidebar',
  imports: [CommonModule],
  templateUrl: './sidebar.html',
  styleUrl: './sidebar.css',
})
export class Sidebar {
  constructor(private router: Router) {}

  navigate(path: string) {
    this.router.navigate([path]);
  }
  @Input() activePage: 'stream-page' | 'streaming' = 'stream-page';
  @Output() pageSelect = new EventEmitter<'stream-page' | 'streaming'>();

  protected readonly menu = [
    { key: 'stream-page' as const, label: 'Stream page' },
    { key: 'streaming' as const, label: 'Streaming' },
  ];

  protected selectPage(page: 'stream-page' | 'streaming'): void {
    this.pageSelect.emit(page);
  }

  isActive(path: string): boolean {
    return this.router.url === path || (path === '/stream' && this.router.url === '/');
  }
}
