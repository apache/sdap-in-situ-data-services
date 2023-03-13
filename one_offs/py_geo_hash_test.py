from collections import defaultdict

import pygeohash


some_dict = defaultdict(list)
for latitude in range(-90, 90):
    for longitude in range(-180, 180):
        some_dict[pygeohash.encode(latitude, longitude, precision=3)].append((latitude, longitude))

for k, v in some_dict.items():
    print(k, len(v))
