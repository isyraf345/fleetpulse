import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './tests', workers: 1, timeout: 45000, retries: 0,
  use: {baseURL: 'http://127.0.0.1:5080', browserName: 'chromium', channel: 'msedge',
    viewport: {width:1440,height:1100}, timezoneId:'Pacific/Auckland', trace:'retain-on-failure'},
  reporter: 'list'
});
