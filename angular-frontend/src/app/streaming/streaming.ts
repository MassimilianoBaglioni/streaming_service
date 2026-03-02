import { Component } from '@angular/core';
import { Button } from '../button/button';
import { invoke } from '@tauri-apps/api/core';

@Component({
  selector: 'app-streaming',
  imports: [Button],
  templateUrl: './streaming.html',
  styleUrl: './streaming.css',
})
export class Streaming {
  async startStreaming() {
    try {
      await invoke('start_streaming');
    } catch (err) {
      console.error('Error invoking rust command: ', err);
    }
  }

  async startListening() {
    try {
      await invoke('start_watching');
    } catch (err) {
      console.error('Error invoking rust command: ', err);
    }
  }
}
