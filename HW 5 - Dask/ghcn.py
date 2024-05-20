#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Parser for GHCN data'''


def load_daily(fname):
    '''Parse a GHCN daily data file into an array of records

    Parameters
    ----------
    fname : str
        Path to a .dly file on disk

    Returns
    -------
    results : list of dict
        Each element of `results` consists of 9 fields:
        - station_id (string) : the identifier of the measurement station
        - year (int)
        - month (int)
        - element (string) : which quantity is being measured
        - day (int)
        - value (int)
        - measurement (string)
        - quality (string)
        - source (string)

        The last three values (measurement, quality, source)
    '''
    # Data format is explained here:
    # https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt

    results = []
    with open(fname, 'r') as fdesc:
        for line in fdesc:
            line = line.rstrip()
            data = dict()

            # First 11 characters are station ID
            data['station_id'] = line[:11]

            # Next 4 characters are the year
            data['year'] = int(line[11:15])

            # Next 2 are the month
            data['month'] = int(line[15:17])

            # Next 4 are the element being measured in this row of the file
            data['element'] = line[17:21]

            # Remaining data corresponds to individual days
            # VVVVVMQS -> value, measurement flag, quality flag, source flag
            chunks = [line[n:n+8] for n in range(21, len(line), 8)]

            for day, chunk in enumerate(chunks, 1):
                day_data = data.copy()
                day_data['day'] = day
                day_data['value'] = int(chunk[:5])
                try:
                    day_data['measurement'], day_data['quality'], day_data['source'] = chunk[5:8]
                except ValueError:
                    # This happens when the number of days is less than 31,
                    # in which case we fill in with blanks
                    day_data['measurement'], day_data['quality'], day_data['source'] = '   '

                results.append(day_data)
    return results
