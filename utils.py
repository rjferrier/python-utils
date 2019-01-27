from typing import NamedTuple, Dict, Tuple, Iterable, Generator
import csv


class Vector(NamedTuple):
    name: str
    values: Tuple


def transpose(records: Iterable[Dict], key_field: str = None) -> Generator[Vector]:
    """
    Given a collection of dictionaries with the same keys, i.e. a collection of records,
    transforms the records into a generator of vectors, with each key corresponding to a vector.
    
    If key_field is given, records will be sorted according to that field.
    
    Example:
        transpose([{'name': 'January', 'id': 1, 'length': 31},
                   {'name': 'February', 'id': 2, 'length': 28},
                   {'name': 'March', 'id': 3, 'length': 31},
                   {'name': 'April', 'id': 4, 'length': 30}], key_field='id')

        -> (Vector('id', (1, 2, 3, 4)),
            Vector('length', (31, 28, 31, 30)),
            Vector('name', ('January', 'February', 'March', 'April')))
    """
    fields = next(iter(records)).keys()
    if key_field:
        records = sorted(records, key=lambda record: record[key_field])
    return (Vector(f, tuple(r[f] for r in records)) for f in fields)


def write_table(filename: str, vectors: Iterable[Vector]) -> None:
    """
    Writes vectors to a CSV file, with the names of the vectors (fields) occupying the first column
    and the values (records) going across in columns.
    """
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        for v in vectors:
            writer.writerow((v.name,) + v.values)
