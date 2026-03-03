import { invoke } from '@tauri-apps/api/core';

export async function callCommand<T = unknown>(
  cmd: string,
  args?: Record<string, unknown>,
): Promise<T | null> {
  try {
    return await invoke<T>(cmd, args);
  } catch (err) {
    console.error(`Error invoking Tauri command "${cmd}":`, err);
    return null;
  }
}
