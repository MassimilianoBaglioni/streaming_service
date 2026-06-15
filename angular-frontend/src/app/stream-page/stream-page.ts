import { Component, ChangeDetectorRef } from '@angular/core';
import { ReactiveFormsModule, FormGroup, FormControl, Validators } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { callCommand } from '../utils/tauri-invoke';
import { listen, UnlistenFn } from '@tauri-apps/api/event';
import { ToastService } from '../services/toast.service';
import { VideoSettingsPage } from '../video-settings-page/video-settings-page';
import { resolve } from '@tauri-apps/api/path';

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
  isWaitingForWatcher = false;
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

  videoForm = new FormGroup({
    fps: new FormControl('30', [Validators.required, Validators.pattern(/^[1-9]\d*$/)]),
    bitrate: new FormControl('5000', [Validators.required, Validators.pattern(/^[1-9]\d*$/)]),
    resolution: new FormControl('1080', [Validators.required, Validators.pattern(/^[1-9]\d*$/)]),
    scalingMethod: new FormControl('Bilinear'),
  });

  get canStartStream(): boolean {
    return this.streamForm.valid && !this.isStreaming && !this.isWaitingForWatcher;
  }

  get canStartWatch(): boolean {
    return this.watchForm.valid && !this.isWatching;
  }

  setMode(newMode: 'streaming' | 'watching'): void {
    this.mode = newMode;
  }

  async startStreaming(): Promise<void> {
    if (!this.canStartStream) return;
    this.isWaitingForWatcher = true;
    this.cdr.markForCheck();

    const videoSettings: StreamVideoSettings = {
      fps: Number(this.videoForm.value.fps),
      resolution: Number(this.videoForm.value.resolution),
      bitrate: Number(this.videoForm.value.bitrate),
      scalingMethod: String(this.videoForm.value.scalingMethod),
    };

    try {
      await callCommand('start_streaming', {
        watcherAddress: this.streamForm.value.watcherAddress,
        streamPort: this.streamForm.value.streamPort,
        tcpPort: this.streamForm.value.tcpPort,
        videoSettings: videoSettings,
      });

      this.isStreaming = true;
      this.startStatusPolling();
      this.cdr.markForCheck();
      console.log('Streaming started');
    } catch (error) {
      console.error('Failed to start streaming:', error);
      this.isStreaming = false;
      this.isWaitingForWatcher = false;
      this.cdr.markForCheck();
    }
  }

  async stopStreaming(): Promise<void> {
    try {
      this.stopStatusPolling();
      await callCommand('stop_streaming');
      this.isStreaming = false;
      this.isWaitingForWatcher = false;
      this.isWatching = false;
      this.cdr.markForCheck();
      console.log('Streaming stopped');
    } catch (error) {
      console.error('Failed to stop streaming:', error);
      this.isStreaming = false;
      this.isWaitingForWatcher = false;
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
      await callCommand('stop_watching', {});
      this.isWatching = false; // ← this is missing
      this.stopStatusPolling();
      this.cleanupWatchListeners();
      this.cdr.markForCheck();
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

export interface StreamVideoSettings {
  fps: number;
  bitrate: number;
  resolution: number;
  scalingMethod: string;
}
