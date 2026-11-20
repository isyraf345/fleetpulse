import {test,expect} from '@playwright/test';

test('synthetic publication flows through PostgreSQL, .NET and Angular',async({page})=>{
  const errors:string[]=[];page.on('pageerror',error=>errors.push(error.message));
  await page.goto('/');
  await expect(page.getByLabel('Run-wide performance')).toContainText('1,440');
  await expect(page.getByLabel('Run-wide performance')).toContainText('19.57%');
  await expect(page.getByLabel('Run-wide performance')).toContainText('25.0%');
  await expect(page.locator('.replay-badge')).toContainText('Historical replay');
  await page.getByRole('link',{name:'Forecast Explorer',exact:true}).click();
  await page.getByLabel('Taxi zone').selectOption({label:'Test zone · Queens'});
  await page.getByRole('button',{name:'Apply filters'}).click();
  await expect(page.getByRole('heading',{name:'Test zone',exact:true})).toBeVisible();
  await expect(page.getByRole('table')).toContainText('2024-06-01 · 00:00');
  await expect(page.getByLabel('Selected window metrics')).toContainText('276');
  await expect(page.getByLabel('Selected window metrics')).toContainText('2.25');
  await page.getByLabel('Until · exclusive').fill('2024-07-01T00:00');
  await page.getByRole('button',{name:'Apply filters'}).click();
  await expect(page.getByText('720 forecast hours',{exact:true})).toBeVisible();
  await expect(page.getByLabel('Selected window metrics')).toContainText('8,280');
  await page.getByRole('button',{name:'Next',exact:true}).click();
  await expect(page.getByText('25–48 of 720 hours')).toBeVisible();
  await expect(page.getByRole('table')).toContainText('2024-06-02 · 00:00');
  await page.getByText('Trace every prediction',{exact:true}).click();
  await expect(page.getByText('local-replay-v1 · local_wall_clock')).toBeVisible();
  await page.screenshot({path:'test-results/ci-explorer.png',fullPage:true});
  expect(errors).toEqual([]);
});

test('API failure and retry do not leave fabricated results',async({page})=>{
  await page.route('**/api/forecast-runs?*',route=>route.fulfill({status:503,json:{detail:'unavailable'}}));
  await page.goto('/');
  await expect(page.getByRole('alert')).toContainText('temporarily unavailable');
  await expect(page.getByLabel('Run-wide performance')).toHaveCount(0);
  await page.unroute('**/api/forecast-runs?*');
  await page.getByRole('button',{name:'Try again',exact:true}).click();
  await expect(page.getByLabel('Run-wide performance')).toContainText('1,440');
});

test('empty publication and unavailable actuals have explicit states',async({page})=>{
  await page.route('**/api/forecast-runs?*',route=>route.fulfill({json:{items:[],activeRunId:null,totalCount:0}}));
  await page.goto('/');
  await expect(page.getByRole('heading',{name:'Your workbench is ready'})).toBeVisible();
  await page.unroute('**/api/forecast-runs?*');
  await page.route('**/predictions?*',async route=>{
    const response=await route.fetch(), body=await response.json();
    await route.fulfill({json:{...body,items:body.items.map((row:object)=>({...row,actualPickups:null}))}});
  });
  await page.goto('/#explorer');await page.reload();
  await expect(page.getByRole('table').getByText('Unavailable')).toHaveCount(24);
  await expect(page.getByLabel('Selected window metrics')).toContainText('—');
});

test('invalid intervals clear results and mobile navigation remains usable',async({page})=>{
  await page.setViewportSize({width:390,height:844});await page.goto('/#explorer');
  await expect(page.getByRole('table')).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  await page.getByLabel('From · local time').fill('2024-06-03T00:00');
  await page.getByRole('button',{name:'Apply filters'}).click();
  await expect(page.getByRole('alert')).toContainText('end after the start');
  await expect(page.getByRole('table')).toHaveCount(0);
  await page.getByRole('link',{name:'Overview',exact:true}).click();
  await expect(page.getByLabel('Run-wide performance')).toContainText('1,440');
});
