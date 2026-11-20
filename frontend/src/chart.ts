import { Component, computed, input, signal, afterNextRender, inject, ElementRef, DestroyRef } from '@angular/core';
import { DecimalPipe } from '@angular/common';
import { Prediction, wallTime } from './models';

@Component({
  selector: 'forecast-chart', standalone: true, imports: [DecimalPipe], host: {style:'display:block'},
  template: `
    <div class="legend"><span><i class="swatch model"></i>Model</span><span><i class="swatch actual"></i>Actual pickups</span><span><i class="swatch baseline"></i>Weekly baseline</span></div>
    <div class="chart-wrap">
      <svg [attr.viewBox]="'0 0 '+width()+' 300'" role="img" aria-labelledby="chart-title chart-description" (pointermove)="inspect($event)">
        <title id="chart-title">Hourly pickup comparison</title>
        <desc id="chart-description">Model, weekly baseline and actual pickups. Use the hour slider or the table below for exact values. Gaps indicate unavailable hours.</desc>
        @for (tick of ticks(); track tick.value) {
          <line x1="55" [attr.x2]="width()-20" [attr.y1]="tick.y" [attr.y2]="tick.y" class="gridline"/>
          <text x="42" [attr.y]="tick.y+4" text-anchor="end" class="axis-label">{{ tick.value | number:'1.0-0' }}</text>
        }
        @for (tick of xTicks(); track tick.index) {
          <text [attr.x]="tick.x" y="290" text-anchor="middle" class="axis-label">{{ tick.label }}</text>
        }
        <path [attr.d]="path('baselinePickups')" class="series baseline-line"/>
        <path [attr.d]="path('actualPickups')" class="series actual-line"/>
        <path [attr.d]="path('predictedPickups')" class="series model-line"/>
        @if (selected(); as row) {
          <line [attr.x1]="x(row)" [attr.x2]="x(row)" y1="16" y2="258" class="cursor-line"/>
          <circle [attr.cx]="x(row)" [attr.cy]="y(row.predictedPickups)" r="5" class="model-point"/>
        }
      </svg>
    </div>
    @if (selected(); as row) {
      <div class="chart-readout"><strong>{{ row.forecastOriginLocal.replace('T',' · ') }}</strong><span>Model <b>{{ row.predictedPickups | number:'1.1-1' }}</b></span><span>Actual <b>{{ row.actualPickups === null ? 'Unavailable' : (row.actualPickups | number) }}</b></span><span>Baseline <b>{{ row.baselinePickups | number:'1.1-1' }}</b></span></div>
      <label class="scrubber">Inspect hour <input type="range" min="0" [max]="rows().length-1" [value]="index()" (input)="index.set(+$any($event.target).value)" aria-label="Inspect forecast hour"/></label>
    }
  `
})
export class ChartComponent {
  width=signal(1000);
  constructor() {
    const host=inject(ElementRef).nativeElement as HTMLElement;
    const destroy=inject(DestroyRef);
    afterNextRender(()=>{
      const observer=new ResizeObserver(()=>this.width.set(Math.max(300,host.clientWidth-32)));
      observer.observe(host);destroy.onDestroy(()=>observer.disconnect());
    });
  }
  rows = input.required<Prediction[]>();
  index = signal(0);
  selected = computed(() => this.rows()[Math.min(this.index(),this.rows().length-1)]);
  maximum = computed(() => {
    const max = Math.max(1,...this.rows().flatMap(r => [r.predictedPickups,r.baselinePickups,r.actualPickups ?? 0]));
    const step = Math.pow(10,Math.floor(Math.log10(max)))/2;
    return Math.ceil(max/step)*step;
  });
  ticks = computed(() => [0,1,2,3,4].map(i => ({ value: this.maximum()*i/4, y: this.y(this.maximum()*i/4) })));
  xTicks = computed(() => [...new Set((this.width()<550?[0,.5,1]:[0,.25,.5,.75,1]).map(n=> Math.round(n*(this.rows().length-1))))].filter(i=>i>=0)
    .map(index => ({index, x:this.x(this.rows()[index]), label:this.rows().length>48 ? this.rows()[index].forecastOriginLocal.slice(5,10)+' '+this.rows()[index].forecastOriginLocal.slice(11,16) : this.rows()[index].forecastOriginLocal.slice(11,16)})));
  x(row: Prediction): number {
    const rows=this.rows(), first=wallTime(rows[0].forecastOriginLocal), last=wallTime(rows[rows.length-1].forecastOriginLocal);
    return last === first ? this.width()/2 : 55 + (wallTime(row.forecastOriginLocal)-first)/(last-first)*(this.width()-75);
  }
  y(value:number):number { return 258-value/this.maximum()*242; }
  path(key:'predictedPickups'|'baselinePickups'|'actualPickups'):string {
    let previous:number|null=null;
    return this.rows().map(row=> {
      const value=row[key], time=wallTime(row.forecastOriginLocal);
      if(value===null) { previous=null; return ''; }
      const command=previous!==null && time-previous===3600000 ? 'L' : 'M'; previous=time;
      return `${command}${this.x(row).toFixed(2)},${this.y(value).toFixed(2)}`;
    }).join(' ');
  }
  inspect(event:PointerEvent) {
    const bounds=(event.currentTarget as SVGElement).getBoundingClientRect();
    const position=Math.max(0,Math.min(1,((event.clientX-bounds.left)/bounds.width*this.width()-55)/(this.width()-75)));
    const rows=this.rows(), start=wallTime(rows[0].forecastOriginLocal), end=wallTime(rows[rows.length-1].forecastOriginLocal);
    const time=start+(end-start)*position;
    this.index.set(rows.reduce((best,row,i)=>Math.abs(wallTime(row.forecastOriginLocal)-time)<Math.abs(wallTime(rows[best].forecastOriginLocal)-time)?i:best,0));
  }
}
