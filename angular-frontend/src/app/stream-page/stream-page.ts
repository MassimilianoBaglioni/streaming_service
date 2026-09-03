import { ChangeDetectorRef, Component } from '@angular/core';
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { callCommand } from '../utils/tauri-invoke';
import { listen, UnlistenFn } from '@tauri-apps/api/event';
import { ToastService } from '../services/toast.service';
import { writeText } from '@tauri-apps/plugin-clipboard-manager';

@Component({
  selector: 'app-stream-page',
  imports: [ReactiveFormsModule, CommonModule],
  templateUrl: './stream-page.html',
  styleUrl: './stream-page.css',
})
export class StreamPage {
  mode: 'streaming' | 'watching' = 'streaming';
  connectionMode: 'direct' | 'invite' = 'direct';
  watchConnectionMode: 'direct' | 'invite' = 'direct';
  isStreaming = false;
  isWatching = false;
  isWaitingForWatcher = false;
  streamForm = new FormGroup({
    watcherAddress: new FormControl('127.0.0.1', [Validators.required]),
    tcpPort: new FormControl('8010', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
    streamPort: new FormControl('5000', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
  });
  watchForm = new FormGroup({
    streamerAddress: new FormControl('127.0.0.1', [Validators.required]),
    inviteLink: new FormControl(''),
    tcpPort: new FormControl('8010', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
    streamPort: new FormControl('5000', [Validators.required, Validators.pattern(/^\d{1,5}$/)]),
  });
  videoForm = new FormGroup({
    fps: new FormControl('30', [Validators.required, Validators.pattern(/^[1-9]\d*$/)]),
    bitrate: new FormControl('5000', [Validators.required, Validators.pattern(/^[1-9]\d*$/)]),
    resolution: new FormControl('1080', [Validators.required, Validators.pattern(/^[1-9]\d*$/)]),
    scalingMethod: new FormControl('Bilinear'),
  });
  inviteTicket: string | null = null;
  isGeneratingTicket = false;
  private statusCheckInterval: any;
  private serverNotStreamingUnlisten: UnlistenFn | null = null;
  private streamingStoppedUnlisten: UnlistenFn | null = null;

  constructor(
    private cdr: ChangeDetectorRef,
    private toastService: ToastService,
  ) {}

  get canStartStream(): boolean {
    if (!this.streamForm.valid || this.isStreaming || this.isWaitingForWatcher) {
      return false;
    }

    return this.connectionMode === 'direct' || !!this.inviteTicket;
  }

  get canStartWatch(): boolean {
    if (this.isWatching) {
      return false;
    }

    const tcpPortValid = this.watchForm.get('tcpPort')?.valid;
    const streamPortValid = this.watchForm.get('streamPort')?.valid;

    if (!tcpPortValid || !streamPortValid) {
      return false;
    }

    if (this.watchConnectionMode === 'direct') {
      return this.watchForm.get('streamerAddress')?.valid ?? false;
    }

    return !!this.watchForm.value.inviteLink?.trim();
  }

  setMode(newMode: 'streaming' | 'watching'): void {
    this.mode = newMode;
  }

  setConnectionMode(newMode: 'direct' | 'invite'): void {
    this.connectionMode = newMode;
  }

  setWatchConnectionMode(newMode: 'direct' | 'invite'): void {
    this.watchConnectionMode = newMode;
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
      if (this.connectionMode === 'direct') {
        await callCommand('start_streaming_direct', {
          watcherAddress: this.streamForm.value.watcherAddress,
          watcherStreamPort: this.streamForm.value.streamPort,
          eventsSocketPort: this.streamForm.value.tcpPort,
          videoSettings: videoSettings,
        });
      } else {
        await callCommand('start_streaming_iroh', {
          videoSettings: videoSettings,
        });
      }

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

  async generateTicket(): Promise<void> {
    this.isGeneratingTicket = true;
    this.cdr.markForCheck();
    try {
      this.inviteTicket = await callCommand<string>('generate_ticket');
    } catch (error) {
      console.error('Failed to generate ticket:', error);
      this.toastService.show('Failed to generate invite link', 'danger');
    } finally {
      this.isGeneratingTicket = false;
      this.cdr.markForCheck();
    }
  }

  async copyTicket(): Promise<void> {
    if (!this.inviteTicket) return;
    await writeText(this.inviteTicket);
    this.toastService.show('Invite link copied!', 'informative');
  }

  async stopStreaming(): Promise<void> {
    try {
      this.stopStatusPolling();
      await callCommand('stop_streaming');
      this.isStreaming = false;
      this.isWaitingForWatcher = false;
      this.isWatching = false;
      console.log('Streaming stopped');
    } catch (error) {
      console.error('Failed to stop streaming:', error);
      this.isStreaming = false;
      this.isWaitingForWatcher = false;
    } finally {
      // Remove any generated invite token so the streamer must regenerate
      // a new token before starting a new session.
      this.inviteTicket = null;
      this.cdr.markForCheck();
    }
  }

  async startWatching(): Promise<void> {
    if (!this.canStartWatch) return;

    try {
      // Clean up old listeners before creating new ones
      this.cleanupWatchListeners();

      const inviteLink = this.watchForm.value.inviteLink?.trim();

      if (this.watchConnectionMode === 'direct') {
        await callCommand('start_watching_direct', {
          streamerIp: this.watchForm.value.streamerAddress ?? '127.0.0.1',
          streamPort: this.watchForm.value.streamPort,
          tcpPort: this.watchForm.value.tcpPort,
        });
      } else {
        await callCommand('start_watching_iroh', {
          ticket: inviteLink,
        });
      }

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
