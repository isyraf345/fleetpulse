export interface Run {
  runId: string; datasetId: string; modelId: string; modelName: string; mode: string; sourceSplit: string;
  originsStartLocal: string; originsEndExclusiveLocal: string; firstObservedLocal: string; lastObservedLocal: string;
  publishedAtUtc: string; predictionCount: number; isActive: boolean; eventTimeBasis: string;
  calendarAssumption: string; featurePolicy: string; freshnessStatus: string; experimentId: string;
  predictionArtifactSha256: string; baselineArtifactSha256: string;
}
export interface Zone { zoneId: number; zoneName: string; borough: string; predictionCount: number; }
export interface Prediction {
  zoneId: number; zoneName: string; borough: string; forecastOriginLocal: string; targetStartLocal: string;
  targetEndLocal: string; predictedPickups: number; baselinePickups: number; actualPickups: number | null;
}
export interface Summary {
  runId: string; rowCount: number; evaluatedCount: number; zoneCount: number; actualPickups: number | null;
  modelMae: number | null; baselineMae: number | null; modelWape: number | null; baselineWape: number | null;
}
export interface PredictionPage { run: Run; items: Prediction[]; totalCount: number; limit: number; offset: number; }
// Arithmetic on offset-free calendar fields only; this does not assign a real UTC origin.
export function wallTime(value: string): number { return Date.parse(value + (value.length === 16 ? ':00Z' : 'Z')); }
export function localAfter(value: string, hours: number): string {
  return new Date(wallTime(value) + hours * 3600000).toISOString().slice(0,16);
}
export function selectionMetrics(rows: Prediction[]) {
  const evaluated = rows.filter(r => r.actualPickups !== null);
  const actual = evaluated.reduce((s,r) => s + r.actualPickups!,0);
  const modelError = evaluated.reduce((s,r) => s + Math.abs(r.predictedPickups-r.actualPickups!),0);
  const baselineError = evaluated.reduce((s,r) => s + Math.abs(r.baselinePickups-r.actualPickups!),0);
  return { evaluatedCount: evaluated.length, actualPickups: evaluated.length ? actual : null,
    modelMae: evaluated.length ? modelError/evaluated.length : null,
    baselineMae: evaluated.length ? baselineError/evaluated.length : null,
    modelWape: actual > 0 ? modelError/actual : null, baselineWape: actual > 0 ? baselineError/actual : null };
}
