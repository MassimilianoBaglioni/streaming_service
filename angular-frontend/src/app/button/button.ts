import { NgClass } from '@angular/common';
import { Component, EventEmitter, Input, Output } from '@angular/core';

@Component({
  selector: 'app-button',
  imports: [NgClass],
  templateUrl: './button.html',
  styleUrl: './button.css',
})
export class Button {
  @Input() variant: 'primary' | 'secondary' | 'danger' = 'primary';
  @Input() text: string = '';

  @Output() clicked = new EventEmitter<void>();

  getClasses(): string {
    switch (this.variant) {
      case 'primary':
        return 'bg-sapphire text-crust hover:bg-sapphire-light';
      case 'secondary':
        return 'bg-mauve text-crust hover:bg-mauve-light';
      case 'danger':
        return 'bg-maroon text-crust hover:bg-red-500';
    }
  }
}
