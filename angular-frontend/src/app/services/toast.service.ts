import { ApplicationRef, Injectable, NgZone } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

export type ToastType = 'informative' | 'danger' | 'warning';

export interface Toast {
  id: string;
  message: string;
  type: ToastType;
  duration: number;
}

@Injectable({ providedIn: 'root' })
export class ToastService {
  private toastsSubject = new BehaviorSubject<Toast[]>([]);
  toasts$ = this.toastsSubject.asObservable();

  constructor(
    private ngZone: NgZone,
    private appRef: ApplicationRef,
  ) {}

  private updateToasts(toasts: Toast[]) {
    this.toastsSubject.next(toasts);
    this.appRef.tick();
  }

  show(message: string, type: ToastType = 'informative', duration = 4000) {
    const toast: Toast = {
      id: crypto.randomUUID(),
      message,
      type,
      duration,
    };

    this.ngZone.run(() => this.updateToasts([...this.toastsSubject.value, toast]));

    this.ngZone.runOutsideAngular(() => {
      setTimeout(() => this.ngZone.run(() => this.remove(toast.id)), duration);
    });
  }

  remove(id: string) {
    this.ngZone.run(() => {
      this.updateToasts(this.toastsSubject.value.filter((t) => t.id !== id));
    });
  }
}
