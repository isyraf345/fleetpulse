"""Approved estimators and training-only preprocessing."""
import numpy as np
from lightgbm import LGBMRegressor
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler

FEATURES = ['zone_id', 'hour_of_day', 'day_of_week', 'lag_1h', 'lag_24h', 'lag_168h', 'mean_previous_24h']


def candidate_specs(plan):
    linear, tree = plan['candidates']
    for alpha in linear['alpha_grid']:
        yield f'poisson_alpha_{alpha:g}', 'poisson', {
            'alpha': alpha, 'solver': linear['solver'], 'max_iter': linear['max_iter'], 'tol': linear['tol']}
    for leaves in tree['num_leaves_grid']:
        params = {key: tree[key] for key in ('objective', 'n_estimators', 'learning_rate',
                  'min_child_samples', 'reg_lambda', 'deterministic', 'force_col_wise', 'random_state')}
        params.update(num_leaves=leaves, n_jobs=tree['num_threads'], verbosity=-1)
        yield f'lightgbm_leaves_{leaves}', 'lightgbm', params


def make_model(kind, params):
    if kind == 'poisson':
        preprocessor = ColumnTransformer([
            ('categories', OneHotEncoder(handle_unknown='ignore', sparse_output=True), [0, 1, 2]),
            ('counts', Pipeline([('log1p', FunctionTransformer(np.log1p)), ('scale', StandardScaler())]), [3, 4, 5, 6]),
        ], sparse_threshold=1.0)
        return Pipeline([('preprocess', preprocessor), ('model', PoissonRegressor(**params))])
    if kind == 'lightgbm':
        return LGBMRegressor(**params)
    raise ValueError(f'Unsupported candidate kind: {kind}')


def predict_model(model, x):
    if x.ndim != 2 or x.shape[1] != len(FEATURES):
        raise ValueError('Prediction input must follow the recorded feature schema')
    if isinstance(model, LGBMRegressor):
        # The schema/order is explicit; use the native predictor for NumPy arrays.
        return model.booster_.predict(x, num_threads=2)
    return model.predict(x)


def passes_gate(candidate, baseline, policy):
    failures = []
    score, reference = candidate['overall'], baseline['overall']
    if (score['rows'] != reference['rows'] or score['actual_sum'] != reference['actual_sum']):
        raise ValueError('Quality gate requires the same evaluation cohort')
    if reference['wape'] is None or reference['wape'] <= 0 or score['wape'] is None:
        failures.append('undefined_relative_improvement')
        improvement = None
    else:
        improvement = 1 - score['wape'] / reference['wape']
        if not np.isfinite(improvement) or improvement < policy['minimum_relative_wape_improvement_over_weekly_baseline']:
            failures.append('overall_improvement')
    groups = {row['group_value']: row for row in candidate['breakdowns']['demand_segment']}
    references = baseline['breakdowns']['demand_segment']
    if set(groups) != {row['group_value'] for row in references}:
        raise ValueError('Quality gate requires identical demand groups')
    for reference_group in references:
        group = reference_group['group_value']
        result = groups[group]
        if result['rows'] != reference_group['rows']:
            raise ValueError('Demand-group cohorts differ')
        limit = reference_group['mae'] * (1 + policy['maximum_relative_mae_increase_in_any_training_defined_demand_segment'])
        if not np.isfinite(result['mae']) or result['mae'] > limit + 1e-12:
            failures.append(f'segment_mae:{group}')
    return {'passed': not failures, 'relative_wape_improvement': improvement, 'failures': failures}


def select_candidate(records):
    eligible = [r for r in records if r['status'] == 'evaluated' and r['gate']['passed']]
    return min(eligible, key=lambda r: (r['metrics']['overall']['wape'], r['name']))['name'] if eligible else 'weekly_baseline'
