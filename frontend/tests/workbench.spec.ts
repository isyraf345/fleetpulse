import { test, expect } from '@playwright/test';

test('real replay overview, zone filtering, full-window pagination, and provenance', async ({page}) => {
  const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto('/');
  await expect(page.getByRole('heading',{name:'A clearer view of demand.'})).toBeVisible();
  await expect(page.getByLabel('Run-wide performance')).toContainText('189,360');
  await expect(page.getByLabel('Run-wide performance')).toContainText('17.23%');
  await expect(page.getByLabel('Run-wide performance')).toContainText('27.8%');
  await expect(page.locator('.replay-badge')).toContainText('Historical replay');
  await page.screenshot({path:'test-results/overview-desktop.png',fullPage:true});
  await page.getByRole('link',{name:'Forecast Explorer',exact:true}).click();
  await expect(page.getByRole('heading',{name:'Explore the hour ahead.'})).toBeVisible();
  await expect(page.getByRole('table')).toContainText('2024-06-01 · 00:00');
  await expect(page.getByRole('table')).toContainText('160.6');
  await expect(page.getByLabel('Selected window metrics')).toContainText('5,119');
  await expect(page.getByLabel('Selected window metrics')).toContainText('37.94');
  await page.getByLabel('Taxi zone').selectOption({label:'Upper East Side North · Manhattan'});
  await page.getByRole('button',{name:'Apply filters'}).click();
  await expect(page.getByRole('heading',{name:'Upper East Side North',exact:true})).toBeVisible();
  await page.getByLabel('Taxi zone').selectOption({label:'JFK Airport · Queens'});
  await page.getByRole('button',{name:'Apply filters'}).click();
  await expect(page.getByRole('heading',{name:'JFK Airport',exact:true})).toBeVisible();
  await page.screenshot({path:'test-results/explorer-desktop.png',fullPage:true});
  await page.getByLabel('Until · exclusive').fill('2024-07-01T00:00');
  await page.getByRole('button',{name:'Apply filters'}).click();
  await expect(page.getByText('720 forecast hours',{exact:true})).toBeVisible();
  await expect(page.getByText('1–24 of 720 hours')).toBeVisible();
  await page.getByRole('button',{name:'Next',exact:true}).click();
  await expect(page.getByText('25–48 of 720 hours')).toBeVisible();
  await expect(page.getByRole('table')).toContainText('2024-06-02 · 00:00');
  await page.getByText('Trace every prediction',{exact:true}).click();
  await expect(page.getByText('2024-06-30 · 23:59',{exact:false})).toBeVisible();
  await expect(page.getByText('local-replay-v1 · local_wall_clock')).toBeVisible();
  expect(errors).toEqual([]);
});

test('reversed local window is rejected without showing stale results',async({page})=>{
  await page.goto('/#explorer');
  await expect(page.getByRole('table')).toBeVisible();
  await page.getByLabel('From · local time').fill('2024-06-03T00:00');
  await page.getByRole('button',{name:'Apply filters'}).click();
  await expect(page.getByRole('alert')).toContainText('end after the start');
  await expect(page.getByRole('table')).toHaveCount(0);
});

test('database error is recoverable and never displays fabricated metrics',async({page})=>{
  await page.route('**/api/forecast-runs?*',route=>route.fulfill({status:503,json:{detail:'unavailable'}}));
  await page.goto('/');
  await expect(page.getByRole('alert')).toContainText('temporarily unavailable');
  await expect(page.getByLabel('Run-wide performance')).toHaveCount(0);
  await page.unroute('**/api/forecast-runs?*');
  await page.getByRole('button',{name:'Try again',exact:true}).click();
  await expect(page.getByLabel('Run-wide performance')).toContainText('189,360');
});

test('empty publication and empty window have distinct honest states',async({page})=>{
  await page.route('**/api/forecast-runs?*',route=>route.fulfill({json:{items:[],totalCount:0,activeRunId:null,limit:500,offset:0}}));
  await page.goto('/');
  await expect(page.getByRole('heading',{name:'Your workbench is ready'})).toBeVisible();
  await page.unroute('**/api/forecast-runs?*');
  await page.route('**/predictions?*',async route=>{const response=await route.fetch();const body=await response.json();await route.fulfill({json:{...body,items:[],totalCount:0}});});
  await page.goto('/#explorer');
  await page.reload();
  await expect(page.getByRole('heading',{name:'No predictions in this window'})).toBeVisible();
  await expect(page.getByRole('table')).toHaveCount(0);
});

test('unavailable actuals remain unavailable and do not become zero-error scores',async({page})=>{
  await page.route('**/predictions?*',async route=>{const response=await route.fetch();const body=await response.json();await route.fulfill({json:{...body,items:body.items.map((row:object)=>({...row,actualPickups:null}))}});});
  await page.goto('/#explorer');
  await expect(page.getByRole('table')).toBeVisible();
  await expect(page.getByRole('table').getByText('Unavailable')).toHaveCount(24);
  await expect(page.getByLabel('Selected window metrics')).toContainText('—');
  await expect(page.getByText('0 of 24 rows have actuals;', {exact:false})).toBeVisible();
});

test('mobile layout keeps controls reachable without page overflow',async({page})=>{
  await page.setViewportSize({width:390,height:844});
  await page.goto('/#explorer');
  await expect(page.getByRole('table')).toBeVisible();
  await expect(page.getByLabel('Taxi zone')).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  await page.screenshot({path:'test-results/explorer-mobile.png',fullPage:true});
  await page.getByRole('link',{name:'Overview',exact:true}).click();
  await expect(page.getByLabel('Run-wide performance')).toContainText('189,360');
});
