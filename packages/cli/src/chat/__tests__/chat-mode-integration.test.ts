import { describe, it, expect, beforeAll } from '@jest/globals';
import { spawn } from 'node-pty';
import fs from 'fs-extra';
import path from 'path';
import { fileURLToPath } from 'url';
import treeKill from 'tree-kill';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CLI_SCRIPT = path.resolve(__dirname, '../../../scripts/onemcp');

// This is a slow end-to-end integration test that depends on external services and
// local handbook configuration. By default we skip it in the normal test run.
// To enable it, run with:
//   RUN_CHAT_INTEGRATION_TEST=true npm test -- src/chat/__tests__/chat-mode-integration.test.ts
const runIntegration = process.env.RUN_CHAT_INTEGRATION_TEST === 'true';

(runIntegration ? describe : describe.skip)('Chat Mode Integration Test', () => {
  const testPrompt = 'Show total sales for 2024.';
  let reportPath: string | null = null;

  beforeAll(async () => {
    try {
      await fs.chmod(CLI_SCRIPT, 0o755);
    } catch (error) {
      // Ignore if chmod fails
    }
  });

  it('should run CLI chat with test prompt and generate a report', async () => {
    // Check prerequisites
    if (!process.env.OPENAI_API_KEY && !process.env.GEMINI_API_KEY) {
      throw new Error('API keys required: Set OPENAI_API_KEY or GEMINI_API_KEY environment variable');
    }

    if (!(await fs.pathExists(CLI_SCRIPT))) {
      throw new Error(`CLI script not found at: ${CLI_SCRIPT}`);
    }

    // Find a handbook to use
    const possibleHandbooks = ['acme-analytics', 'acme'];
    let handbookToUse: string | undefined;
    
    const handbooksDirs = [
      path.join(process.env.HOME || '', '.onemcp', 'handbooks'),
      path.join(process.env.HOME || '', 'handbooks'),
    ];

    for (const handbooksDir of handbooksDirs) {
      try {
        if (await fs.pathExists(handbooksDir)) {
          const handbookDirs = await fs.readdir(handbooksDir);
          const found = possibleHandbooks.find(h => handbookDirs.includes(h));
          if (found) {
            handbookToUse = found;
            break;
          }
        }
      } catch (error) {
        // Continue searching
      }
    }

    const chatCommand = handbookToUse ? ['chat', handbookToUse] : ['chat'];
    let ptyRef: any = null;

    const promise = new Promise<void>((resolve, reject) => {
      let pty: any;
      let resolved = false;
      let rejected = false;
      const timeouts: NodeJS.Timeout[] = [];
      const intervals: NodeJS.Timeout[] = [];
      let ptyPid: number | undefined;

      const safeResolve = () => {
        if (!resolved && !rejected) {
          resolved = true;
          cleanup();
          resolve();
        }
      };

      const safeReject = (error: Error) => {
        if (!resolved && !rejected) {
          rejected = true;
          cleanup();
          reject(error);
        }
      };

      const cleanup = () => {
        intervals.forEach(i => clearInterval(i));
        timeouts.forEach(t => clearTimeout(t));
        if (ptyPid) {
          try {
            treeKill(ptyPid, 'SIGKILL', () => {});
          } catch (error) {
            // Ignore
          }
        }
        try {
          if (pty) {
            pty.kill();
            if (typeof pty.destroy === 'function') {
              pty.destroy();
            }
          }
        } catch (error) {
          // Ignore
        }
      };

      try {
        pty = spawn('bash', [CLI_SCRIPT, ...chatCommand], {
          name: 'xterm-color',
          cols: 80,
          rows: 24,
          cwd: process.cwd(),
          env: process.env as Record<string, string>,
        });
        ptyRef = pty;
      } catch (error: any) {
        safeReject(new Error(`Failed to spawn PTY: ${error.message}`));
        return;
      }

      let output = '';
      let promptReceived = false;
      let messageSent = false;
      let exitSent = false;
      let responseReceived = false;

      // Get PID
      setTimeout(() => {
        try {
          ptyPid = pty.pid;
        } catch (error) {
          // Ignore
        }
      }, 100);

      // Main timeout
      timeouts.push(setTimeout(() => {
        cleanup();
        safeReject(new Error('Test timeout after 5 minutes'));
      }, 300000));

      // Prompt timeout
      timeouts.push(setTimeout(() => {
        if (!promptReceived) {
          cleanup();
          safeReject(new Error('CLI did not show prompt within 30 seconds'));
        }
      }, 30000));

      // Validate report function
      const validateReport = async (reportPathToValidate: string): Promise<void> => {
        const cleanPath = reportPathToValidate
          .replace(/\u001b\[[0-9;]*m/g, '')
          .trim()
          .replace(/[\r\n]+$/, '');

        expect(cleanPath).toBeTruthy();
        expect(cleanPath).toContain('execution-');
        expect(cleanPath.endsWith('.txt')).toBe(true);
        expect(path.isAbsolute(cleanPath)).toBe(true);

        const reportExists = await fs.pathExists(cleanPath);
        expect(reportExists).toBe(true);

        const reportContent = await fs.readFile(cleanPath, 'utf-8');
        expect(reportContent.trim().length).toBeGreaterThan(0);
        expect(reportContent).toContain('EXECUTION REPORT');
        expect(reportContent).toContain('EXECUTION SUMMARY');
        expect(reportContent).toContain(testPrompt);
        expect(reportContent).toContain('END OF REPORT');
        expect(reportContent).toMatch(/Timestamp:\s+/);
        expect(reportContent).toMatch(/Duration:\s+\d+ms/);
        expect(reportContent).toMatch(/Status:\s+\[(SUCCESS|FAILED)\]/);
        expect(reportContent).toContain('API Calls:');
        expect(reportContent).toContain('Errors:');
      };

      // Extract report path from output or filesystem
      const findReportPath = async (): Promise<string | null> => {
        const cleanOutput = output.replace(/\u001b\[[0-9;]*m/g, '');
        let match = cleanOutput.match(/Report:\s+([^\s\n]+)/);
        
        if (match?.[1]) {
          return match[1].trim();
        }

        match = cleanOutput.match(/(\/[^\s\n]*execution-[^\s\n]*\.txt)/);
        if (match?.[1]) {
          return match[1].trim();
        }

        // Search filesystem
        const possibleDirs = [
          path.join(process.env.HOME || '', '.onemcp', 'logs', 'reports'),
        ];

        for (const baseDir of handbooksDirs) {
          try {
            if (await fs.pathExists(baseDir)) {
              const handbookDirs = await fs.readdir(baseDir);
              for (const handbookDir of handbookDirs) {
                const reportsDir = path.join(baseDir, handbookDir, 'logs', 'reports');
                if (await fs.pathExists(reportsDir)) {
                  possibleDirs.push(reportsDir);
                }
              }
            }
          } catch (error) {
            // Continue
          }
        }

        for (const reportsDir of possibleDirs) {
          try {
            if (await fs.pathExists(reportsDir)) {
              const files = await fs.readdir(reportsDir);
              const reportFiles = files
                .filter(f => f.startsWith('execution-') && f.endsWith('.txt'))
                .map(f => path.join(reportsDir, f));
              
              if (reportFiles.length > 0) {
                // Get most recent
                const stats = await Promise.all(
                  reportFiles.map(async (f) => ({
                    path: f,
                    mtime: (await fs.stat(f)).mtime.getTime(),
                  }))
                );
                stats.sort((a, b) => b.mtime - a.mtime);
                const recent = stats.find(s => (Date.now() - s.mtime) < 600000);
                return (recent || stats[0])?.path || null;
              }
            }
          } catch (error) {
            // Continue
          }
        }

        return null;
      };

      // Data handler
      pty.onData((data: string) => {
        output += data;

        // Wait for prompt, then send test prompt
        if (!promptReceived && (data.includes('You:') || data.includes('? You:'))) {
          promptReceived = true;
          setTimeout(() => {
            if (!messageSent) {
              pty.write(testPrompt + '\r');
              messageSent = true;
            }
          }, 1000);
        }

        // Detect response indicators
        if (messageSent && !responseReceived) {
          if (data.includes('Agent:') || data.includes('Response received') || 
              data.includes('Report:') || data.includes('Thinking...')) {
            responseReceived = true;
          }
        }

        // After we get a response and see the next prompt, send exit
        if (messageSent && responseReceived && !exitSent && 
            (data.includes('You:') || data.includes('? You:'))) {
          exitSent = true;
          setTimeout(() => {
            pty.write('exit\r');
            
            // Fallback: if process doesn't exit within 30 seconds, force kill
            timeouts.push(setTimeout(() => {
              if (ptyPid) {
                try {
                  process.kill(ptyPid, 0);
                  // Still alive, force cleanup
                  cleanup();
                  if (!resolved && !rejected) {
                    safeReject(new Error('Process did not exit after exit command'));
                  }
                } catch (error: any) {
                  // Process already dead, that's fine
                }
              }
            }, 30000));
          }, 1000);
        }
      });

      // Exit handler
      pty.onExit(async () => {
        cleanup();

        if (resolved || rejected) {
          return;
        }

        try {
          // Give a moment for final output
          await new Promise(resolve => setTimeout(resolve, 500));
          
          reportPath = await findReportPath();
          if (!reportPath) {
            safeReject(new Error('Report path not found'));
            return;
          }

          await validateReport(reportPath);
          
          // Print the report file location
          console.log('\nReport file:', reportPath + '\n');
          
          safeResolve();
        } catch (error: any) {
          safeReject(error);
        }
      });

      // Safety timeout in case onExit never fires
      timeouts.push(setTimeout(() => {
        if (!resolved && !rejected) {
          cleanup();
          safeReject(new Error('Process did not exit - onExit never fired'));
        }
      }, 310000));
    });

    // Wrap with timeout
    let raceTimeout: NodeJS.Timeout | null = null;
    const racePromise = Promise.race([
      promise,
      new Promise<void>((_, reject) => {
        raceTimeout = setTimeout(() => {
          reject(new Error('Test promise timeout'));
        }, 310000);
      }),
    ]);

    return racePromise.finally(async () => {
      if (raceTimeout) {
        clearTimeout(raceTimeout);
      }

      // Final cleanup
      if (ptyRef) {
        try {
          const pid = ptyRef.pid;
          if (pid) {
            await new Promise<void>((resolve) => {
              const killTimeout = setTimeout(resolve, 5000);
              treeKill(pid, 'SIGKILL', () => {
                clearTimeout(killTimeout);
                resolve();
              });
            });
          }
          ptyRef.kill();
          if (typeof (ptyRef as any).destroy === 'function') {
            (ptyRef as any).destroy();
          }
        } catch (error) {
          // Ignore
        }
        ptyRef = null;
      }

      // Clean up any remaining socket handles
      const activeHandles = (process as any)._getActiveHandles ? (process as any)._getActiveHandles() : [];
      activeHandles.forEach((handle: any) => {
        if ((handle.constructor?.name === 'Socket' || handle.constructor?.name === 'Server') &&
            !handle.destroyed && typeof handle.destroy === 'function') {
          try {
            handle.destroy();
            if (handle.removeAllListeners) {
              handle.removeAllListeners();
            }
          } catch (error) {
            // Ignore
          }
        }
      });
    });
  }, 300000);
});
