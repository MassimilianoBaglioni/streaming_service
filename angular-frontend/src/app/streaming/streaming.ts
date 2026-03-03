import { Component } from '@angular/core';
import { Button } from '../button/button';
import { invoke } from '@tauri-apps/api/core';
import { callCommand } from '../utils/tauri-invoke';

@Component({
  selector: 'app-streaming',
  imports: [Button],
  templateUrl: './streaming.html',
  styleUrl: './streaming.css',
})
export class Streaming {
  async startStreaming() {
    callCommand('start_streaming');
  }

  async startListening() {
    callCommand('start_watching');
  }

  async stopStreaming() {
    callCommand('stop_streaming');
  }
}
