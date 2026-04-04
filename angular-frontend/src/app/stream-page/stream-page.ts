import { Component } from '@angular/core';
import { ReactiveFormsModule, FormGroup, FormControl, Validators } from '@angular/forms';
import { CommonModule } from '@angular/common';
@Component({
  selector: 'app-stream-page',
  imports: [ReactiveFormsModule, CommonModule],
  templateUrl: './stream-page.html',
  styleUrl: './stream-page.css',
})
export class StreamPage {
  form = new FormGroup({
    host: new FormControl('192.168.1.42', [Validators.required]),
    port: new FormControl('5000', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
  });
  get canStart(): boolean {
    return this.sourceSelected && this.form.valid;
  }
  sourceSelected = false;
  startStream(): void {
    if (!this.canStart) return;
    const { host, port } = this.form.value;
    console.log(`Starting stream to ${host}:${port}`);
  }
  pickSource(): void {
    this.sourceSelected = !this.sourceSelected;
  }
}
