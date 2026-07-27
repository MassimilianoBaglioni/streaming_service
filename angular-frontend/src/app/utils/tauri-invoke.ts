import { invoke } from '@tauri-apps/api/core';

export async function callCommand<T = unknown>(
  cmd: string,
  args?: Record<string, unknown>,
): Promise<T> {
  return await invoke<T>(cmd, args);
}
