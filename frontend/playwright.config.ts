import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './tests', workers: 1, timeout: 45000, retries: 0,
  testMatch: process.env.FLEETPULSE_E2E_FIXTURE ? 'ci.spec.ts' : 'workbench.spec.ts',
  forbidOnly: !!process.env.CI,
  use: {baseURL: process.env.FLEETPULSE_BASE_URL || 'http://127.0.0.1:5080', browserName: 'chromium',
    channel: process.env.FLEETPULSE_BROWSER_CHANNEL || (process.env.CI ? undefined : 'msedge'),
    viewport: {width:1440,height:1100}, timezoneId:'Pacific/Auckland', trace:'retain-on-failure'},
  reporter: 'list'
});
