import { Component, ChangeDetectorRef } from '@angular/core';
import { ReactiveFormsModule, FormGroup, FormControl, Validators } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { callCommand } from '../utils/tauri-invoke';
import { listen, UnlistenFn } from '@tauri-apps/api/event';
import { ToastService } from '../services/toast.service';

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
  private statusCheckInterval: any;
  private serverNotStreamingUnlisten: UnlistenFn | null = null;
  private streamingStoppedUnlisten: UnlistenFn | null = null;

  constructor(
    private cdr: ChangeDetectorRef,
    private toastService: ToastService,
  ) {}

  streamForm = new FormGroup({
    watcherAddress: new FormControl('127.0.0.1', [Validators.required]),
    tcpPort: new FormControl('8010', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
    streamPort: new FormControl('5000', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
  });

  watchForm = new FormGroup({
    streamerAddress: new FormControl('127.0.0.1', [Validators.required]),
    tcpPort: new FormControl('8010', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
    streamPort: new FormControl('5000', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
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
      await callCommand('start_streaming', {
        watcherAddress: this.streamForm.value.watcherAddress,
        streamPort: this.streamForm.value.streamPort,
        tcpPort: this.streamForm.value.tcpPort,
      });
      this.isStreaming = true;
      this.startStatusPolling();
      this.cdr.markForCheck();
      console.log('Streaming started');
    } catch (error) {
      console.error('Failed to start streaming:', error);
      this.isStreaming = false;
      this.cdr.markForCheck();
    }
  }

  async stopStreaming(): Promise<void> {
    try {
      this.stopStatusPolling();
      await callCommand('stop_streaming');
      this.isStreaming = false;
      this.isWatching = false;
      this.cdr.markForCheck();
      console.log('Streaming stopped');
    } catch (error) {
      console.error('Failed to stop streaming:', error);
      this.cdr.markForCheck();
    }
  }

  async startWatching(): Promise<void> {
    if (!this.canStartWatch) return;

    try {
      // Clean up old listeners before creating new ones
      this.cleanupWatchListeners();

      await callCommand('start_watching', {
        streamerAddress: this.watchForm.value.streamerAddress,
        streamPort: this.watchForm.value.streamPort,
        tcpPort: this.watchForm.value.tcpPort,
        streamerIp: this.watchForm.value.streamerAddress,
      });
      this.isWatching = true;

      console.log('Watching started');

      this.serverNotStreamingUnlisten = await listen('server-not-streaming', () => {
        this.toastService.show('Server is not streaming', 'danger');
        this.cdr.markForCheck();
      });

      this.streamingStoppedUnlisten = await listen('streaming-stopped', () => {
        this.isWatching = false;
        this.cleanupWatchListeners();
        this.cdr.markForCheck();
      });

      this.cdr.markForCheck();
    } catch (error) {
      console.error('Failed to start watching:', error);
      this.isWatching = false;
      this.cdr.markForCheck();
    }
  }

  async stopWatching(): Promise<void> {
    try {
      this.stopStatusPolling();
      this.cleanupWatchListeners();

      this.cdr.markForCheck();
      console.log('Watching stopped');
    } catch (error) {
      console.error('Failed to stop watching:', error);
      this.cdr.markForCheck();
    }
  }

  private cleanupWatchListeners(): void {
    if (this.serverNotStreamingUnlisten) {
      this.serverNotStreamingUnlisten();
      this.serverNotStreamingUnlisten = null;
    }
    if (this.streamingStoppedUnlisten) {
      this.streamingStoppedUnlisten();
      this.streamingStoppedUnlisten = null;
    }
  }

  private startStatusPolling(): void {
    this.stopStatusPolling(); // Clear any existing polling

    this.statusCheckInterval = setInterval(async () => {
      try {
        // Try to call a command that would fail if streaming isn't active
        // This is a simple way to detect if the stream is still running
        // If the stream has stopped on the backend, the next call might fail or return an error
        if (!this.isStreaming && !this.isWatching) {
          this.stopStatusPolling();
        }
      } catch (error) {
        console.log('Stream status check error (stream may have stopped)');
      }
    }, 2000); // Check every 2 seconds
  }

  private stopStatusPolling(): void {
    if (this.statusCheckInterval) {
      clearInterval(this.statusCheckInterval);
      this.statusCheckInterval = null;
    }
  }
}
