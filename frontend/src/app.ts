import { Component, HostListener, computed, signal } from '@angular/core';
import { DecimalPipe, PercentPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ChartComponent } from './chart';
import { Run, Zone, Summary, Prediction, PredictionPage, localAfter, wallTime, selectionMetrics } from './models';

@Component({ selector: 'app-root', standalone: true, imports: [FormsModule,DecimalPipe,PercentPipe,ChartComponent], templateUrl: './app.html' })
export class AppComponent {
  view=signal(location.hash==='#explorer'?'explorer':'overview');
  runs=signal<Run[]>([]); run=signal<Run|null>(null); zones=signal<Zone[]>([]); summary=signal<Summary|null>(null);
  rows=signal<Prediction[]>([]); loading=signal(true); querying=signal(false); error=signal(''); queryError=signal('');
  selectedRun=''; zoneId=132; fromLocal=''; toLocal=''; applied=signal<{zone:string;from:string;to:string}|null>(null);
  rowPage=signal(0); pageSize=24; request?:AbortController; predictionRequest?:AbortController;
  metrics=computed(()=>selectionMetrics(this.rows()));
  visibleRows=computed(()=>this.rows().slice(this.rowPage()*this.pageSize,(this.rowPage()+1)*this.pageSize));
  improvement=computed(()=> {const s=this.summary();return s?.modelWape!==null && s?.baselineWape ? 1-s!.modelWape!/s.baselineWape : null;});
  Math=Math;
  constructor() { void this.initialize(); }
  @HostListener('window:hashchange') hashChanged() {this.view.set(location.hash==='#explorer'?'explorer':'overview');}
  async get<T>(path:string,signal?:AbortSignal):Promise<T> {
    const response=await fetch('/api/forecast-runs'+path,{signal});
    if(!response.ok) throw new Error(response.status===503?'The forecast database is unavailable. Try again when the service is ready.':response.status===404?'This forecast run is no longer available. Refresh the run list.':'We could not load the forecast data. Please try again.');
    return response.json();
  }
  async initialize() {
    this.request?.abort(); this.predictionRequest?.abort();
    const request=new AbortController();this.request=request;this.loading.set(true);this.error.set('');this.run.set(null);this.rows.set([]);this.summary.set(null);
    try {
      const page=await this.get<{items:Run[];activeRunId:string|null}>('?limit=500',request.signal);
      if(page.activeRunId && !page.items.some(r=>r.runId===page.activeRunId)) page.items.unshift(await this.get<Run>('/'+page.activeRunId,request.signal));
      if(request.signal.aborted)return;
      this.runs.set(page.items);
      if(page.items.length) {this.selectedRun=page.activeRunId ?? page.items[0].runId;await this.loadRun();}
    } catch(e) {if(!request.signal.aborted)this.error.set(this.message(e));}
    finally {if(this.request===request)this.loading.set(false);}
  }
  async loadRun() {
    this.request?.abort();this.predictionRequest?.abort();const request=new AbortController();this.request=request;
    this.loading.set(true);this.error.set('');this.rows.set([]);this.applied.set(null);this.summary.set(null);this.run.set(null);this.queryError.set('');
    try {
      const id=this.selectedRun;
      const [run,zones,summary]=await Promise.all([this.get<Run>('/'+id,request.signal),this.get<{items:Zone[]}>('/'+id+'/zones',request.signal),this.get<Summary>('/'+id+'/summary',request.signal)]);
      if(request.signal.aborted)return;
      this.run.set(run);this.zones.set(zones.items);this.summary.set(summary);
      if(!zones.items.some(z=>z.zoneId===this.zoneId))this.zoneId=zones.items[0]?.zoneId ?? 0;
      this.fromLocal=run.originsStartLocal.slice(0,16);
      this.toLocal=localAfter(run.originsStartLocal,Math.min(24,(wallTime(run.originsEndExclusiveLocal)-wallTime(run.originsStartLocal))/3600000));
      if(zones.items.length)await this.apply();
    }catch(e){if(!request.signal.aborted)this.error.set(this.message(e));}
    finally{if(this.request===request)this.loading.set(false);}
  }
  async apply() {
    this.predictionRequest?.abort();const request=new AbortController();this.predictionRequest=request;
    this.queryError.set('');this.rows.set([]);this.applied.set(null);this.rowPage.set(0);this.querying.set(false);
    const run=this.run();if(!run)return;
    const valid=(v:string)=> /^\d{4}-\d{2}-\d{2}T\d{2}:00$/.test(v) && Number.isFinite(wallTime(v)) && new Date(wallTime(v)).toISOString().slice(0,16)===v;
    if(!valid(this.fromLocal)||!valid(this.toLocal)||this.fromLocal>=this.toLocal){this.queryError.set('Choose valid whole hours, with the end after the start.');return;}
    if(this.fromLocal<run.originsStartLocal.slice(0,16)||this.toLocal>run.originsEndExclusiveLocal.slice(0,16)){this.queryError.set('Choose a time window within this run’s replay period.');return;}
    if(wallTime(this.toLocal)-wallTime(this.fromLocal)>31*86400000){this.queryError.set('Choose a window of 31 days or less.');return;}
    if(!this.zones().some(z=>z.zoneId===+this.zoneId)){this.queryError.set('Choose an available taxi zone.');return;}
    const zone=this.zones().find(z=>z.zoneId===+this.zoneId)!;
    const selected={zone:zone.zoneName,from:this.fromLocal,to:this.toLocal};
    this.querying.set(true);
    try {
      const rows:Prediction[]=[];let total=0;
      do {
        const query=new URLSearchParams({zoneId:String(zone.zoneId),fromLocal:selected.from+':00',toLocal:selected.to+':00',limit:'500',offset:String(rows.length)});
        const page=await this.get<PredictionPage>('/'+run.runId+'/predictions?'+query,request.signal);
        total=page.totalCount;rows.push(...page.items);
        if(page.items.length===0 && rows.length<total)throw new Error('The forecast response was incomplete. Please try again.');
      }while(rows.length<total);
      if(request.signal.aborted)return;
      this.rows.set(rows);this.applied.set(selected);
    }catch(e){if(!request.signal.aborted)this.queryError.set(this.message(e));}
    finally{if(this.predictionRequest===request)this.querying.set(false);}
  }
  message(error:unknown){return error instanceof TypeError?'Cannot reach the forecast API. Check the local service and try again.':error instanceof Error?error.message:'Something went wrong. Please try again.';}
  local(value:string){return value.replace('T',' · ').slice(0,18);}
  localExact(value:string){return value.replace('T',' · ');}
  utc(value:string){return new Date(value).toISOString().replace('T',' ').slice(0,19)+' UTC';}
  short(value:string){return value.slice(0,12);}
  modelLabel(value:string){return value==='lightgbm_leaves_31'?'LightGBM · 31 leaves':value==='weekly_baseline'?'Weekly baseline':value;}
  dayWindow(){const run=this.run();if(run){this.fromLocal=run.originsStartLocal.slice(0,16);this.toLocal=localAfter(run.originsStartLocal,24);void this.apply();}}
}
