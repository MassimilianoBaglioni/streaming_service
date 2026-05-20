import { Component, OnInit, NgZone } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Toast, ToastService } from '../../services/toast.service';
import { trigger, style, transition, animate } from '@angular/animations';
@Component({
  selector: 'app-toast',
  standalone: true,
  imports: [CommonModule],
  animations: [
    trigger('slideUp', [
      transition(':enter', [
        style({ transform: 'translateY(100%)', opacity: 0 }),
        animate(
          '400ms cubic-bezier(0.34, 1.56, 0.64, 1)',
          style({ transform: 'translateY(0)', opacity: 1 }),
        ),
      ]),
      transition(':leave', [
        animate('250ms ease-in', style({ transform: 'translateY(30px)', opacity: 0 })),
      ]),
    ]),
  ],
  template: `
    <div class="toast-container">
      <div
        *ngFor="let toast of toasts; trackBy: trackById"
        [@slideUp]
        [class]="'toast toast--' + toast.type"
        (click)="toastService.remove(toast.id)"
      >
        <span class="toast__icon">{{ icons[toast.type] }}</span>
        <span class="toast__message">{{ toast.message }}</span>
        <div
          class="toast__progress"
          [style.animation-duration.ms]="toast.duration"
          (animationend)="onProgressEnd(toast.id)"
        ></div>
      </div>
    </div>
  `,
  styles: [
    `
      .toast-container {
        position: fixed;
        bottom: 24px;
        left: 50%;
        transform: translateX(-50%);
        display: flex;
        flex-direction: column-reverse;
        gap: 10px;
        z-index: 9999;
        align-items: center;
        pointer-events: none;
      }

      .toast {
        pointer-events: all;
        position: relative;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 12px;
        width: min(300px, calc(100vw - 32px));
        padding: 14px 40px;
        border-radius: var(--r10);
        background: rgba(28, 28, 34, 0.95);
        color: var(--t1);
        border: 1px solid rgba(255, 255, 255, 0.08);
        font-family: var(--font-sans);
        font-size: 14px;
        font-weight: 500;
        cursor: pointer;
        overflow: hidden;
        box-shadow: 0 24px 70px rgba(0, 0, 0, 0.32);
        backdrop-filter: blur(18px);
        text-align: center;
      }

      .toast--informative,
      .toast--info {
        border-color: rgba(124, 111, 247, 0.25);
      }
      .toast--danger,
      .toast--error {
        border-color: rgba(226, 75, 74, 0.35);
      }
      .toast--warning {
        border-color: rgba(239, 159, 39, 0.25);
      }

      .toast__icon {
        position: absolute;
        left: 18px;
        top: 50%;
        transform: translateY(-50%);
        display: inline-flex;
        align-items: center;
        justify-content: center;
        color: inherit;
        font-size: 18px;
        line-height: 1;
        width: 22px;
        height: 22px;
        padding: 0;
        background: none;
        flex-shrink: 0;
      }

      .toast--informative .toast__icon,
      .toast--info .toast__icon {
        color: #7c6ff7;
      }
      .toast--danger .toast__icon,
      .toast--error .toast__icon {
        color: #e24b4a;
      }
      .toast--warning .toast__icon {
        color: #ef9f27;
      }

      .toast__message {
        flex: 1;
        line-height: 1.6;
        text-align: center;
        min-width: 0;
      }

      .toast__progress {
        position: absolute;
        bottom: 0;
        left: 0;
        height: 3px;
        width: 100%;
        animation: shrink linear forwards;
        transform-origin: left;
        background: rgba(255, 255, 255, 0.12);
      }

      .toast--informative .toast__progress,
      .toast--info .toast__progress {
        background: #7c6ff7;
      }
      .toast--danger .toast__progress,
      .toast--error .toast__progress {
        background: #e24b4a;
      }
      .toast--warning .toast__progress {
        background: #ef9f27;
      }

      @keyframes shrink {
        from {
          transform: scaleX(1);
        }
        to {
          transform: scaleX(0);
        }
      }
    `,
  ],
})
export class ToastComponent implements OnInit {
  toasts: Toast[] = [];

  icons: Record<string, string> = {
    informative: 'ℹ',
    danger: '⚠',
    warning: '⚠',
    info: 'ℹ',
    error: '✕',
    success: '✓',
  };

  constructor(
    public toastService: ToastService,
    private zone: NgZone,
  ) {}

  ngOnInit() {
    this.toastService.toasts$.subscribe((t) => {
      this.zone.run(() => (this.toasts = t));
    });
  }

  onProgressEnd(id: string) {
    this.toastService.remove(id);
  }

  trackById(_: number, toast: Toast) {
    return toast.id;
  }
}
