import numpy as np
import matplotlib.pyplot as plt

## plot histogram
bins = [1, 2, 5, 10, 20, 50, 100, 1000]
bin_labels = ["1", "2-5", "5-10", "10-20", "20-50", "50-100", "100-1000"]

hist_values = np.histogram(n_purchased_listings_by_query, bins=bins)[0]
percentage = [
    "{0:.0%}".format(x) for x in np.round(hist_values / n_distinct_queries, 5)
]

ax = plt.bar(x=bin_labels, height=hist_values)
for x, y, p in zip(bin_labels, hist_values, percentage):
    plt.text(x, y, p)
plt.ylabel("Count of queries")
plt.xlabel("N puchased listings")
plt.title("Count and % of queries with N purchased listings")
plt.show()
