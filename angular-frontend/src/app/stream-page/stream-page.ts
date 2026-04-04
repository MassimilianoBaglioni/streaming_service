import { Component } from '@angular/core';
import { ReactiveFormsModule, FormGroup, FormControl, Validators } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { callCommand } from '../utils/tauri-invoke';

@Component({
  selector: 'app-stream-page',
  imports: [ReactiveFormsModule, CommonModule],
  templateUrl: './stream-page.html',
  styleUrl: './stream-page.css',
})
export class StreamPage {
  mode: 'streaming' | 'watching' = 'streaming';
  isStreaming = false;
  isWatching = false;

  streamForm = new FormGroup({
    tcpPort: new FormControl('5000', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
    streamPort: new FormControl('8000', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
  });

  watchForm = new FormGroup({
    streamerAddress: new FormControl('', [Validators.required]),
    tcpPort: new FormControl('5000', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
    streamPort: new FormControl('8000', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
  });

  get canStartStream(): boolean {
    return this.streamForm.valid && !this.isStreaming;
  }

  get canStartWatch(): boolean {
    return this.watchForm.valid && !this.isWatching;
  }

  setMode(newMode: 'streaming' | 'watching'): void {
    this.mode = newMode;
  }

  async startStreaming(): Promise<void> {
    if (!this.canStartStream) return;

    try {
      await callCommand('start_streaming');
      this.isStreaming = true;
      console.log('Streaming started');
    } catch (error) {
      console.error('Failed to start streaming:', error);
      this.isStreaming = false;
    }
  }

  async stopStreaming(): Promise<void> {
    try {
      await callCommand('stop_streaming');
      this.isStreaming = false;
      console.log('Streaming stopped');
    } catch (error) {
      console.error('Failed to stop streaming:', error);
    }
  }

  async startWatching(): Promise<void> {
    if (!this.canStartWatch) return;

    try {
      await callCommand('start_watching');
      this.isWatching = true;
      console.log('Watching started');
    } catch (error) {
      console.error('Failed to start watching:', error);
      this.isWatching = false;
    }
  }
}
