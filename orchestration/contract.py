"""Small, dependency-free validation shared by DAGs and command-line adapters."""
from datetime import datetime
from pathlib import Path
import re

APPROVED_EXPERIMENT = '2c35384e7c539f1ced3d3591ece63b39cea75eb7936e4b9cffca666c4d8adf1e'


def months_between(start, end):
    for value in (start, end):
        if not isinstance(value, str) or not re.fullmatch(r'20\d{2}-(0[1-9]|1[0-2])', value):
            raise ValueError('Use a calendar month in YYYY-MM format')
    first, last = (datetime.strptime(value, '%Y-%m') for value in (start, end))
    count = (last.year-first.year)*12 + last.month-first.month + 1
    if not 1 <= count <= 12:
        raise ValueError('Choose an ordered range of at most 12 months')
    return [f'{(first.year*12+first.month-1+i)//12:04d}-{(first.month-1+i)%12+1:02d}' for i in range(count)]


def artifact_id(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValueError('Artifact ID must be a lowercase SHA-256 identifier')
    return value


def scoped_path(root, value):
    root = Path(root).resolve()
    path = Path(value).resolve()
    if path == root or not path.is_relative_to(root):
        raise ValueError('Artifact must be inside the orchestrated output root')
    return path
