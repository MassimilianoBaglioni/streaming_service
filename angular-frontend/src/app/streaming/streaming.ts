import { Component } from '@angular/core';
import { Button } from '../button/button';
import { CommonModule } from '@angular/common';
import { callCommand } from '../utils/tauri-invoke';

@Component({
  selector: 'app-streaming',
  imports: [Button, CommonModule],
  templateUrl: './streaming.html',
  styleUrl: './streaming.css',
})
export class Streaming {
  isStreaming = false;
  isWatching = false;

  async startStreaming(): Promise<void> {
    try {
      await callCommand('start_streaming');
      this.isStreaming = true;
    } catch (error) {
      console.error('Failed to start streaming:', error);
    }
  }

  async startListening(): Promise<void> {
    try {
      await callCommand('start_watching');
      this.isWatching = true;
    } catch (error) {
      console.error('Failed to start watching:', error);
    }
  }

  async stopStreaming(): Promise<void> {
    try {
      await callCommand('stop_streaming');
      this.isStreaming = false;
      this.isWatching = false;
    } catch (error) {
      console.error('Failed to stop streaming:', error);
    }
  }
}
