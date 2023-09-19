# cython: language_level=3
import time
import numpy as np
cimport numpy as np  # This is the C-level import for NumPy
from libc.stdlib cimport abs
from datetime import datetime

# Define the timing decorator
def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.5f} seconds to run.")
        return result
    return wrapper

# Apply the timing decorator to the function
@timing_decorator
def assign_equity_prices_at_915_cython(np.ndarray original_df, np.ndarray batch_data_df, str current_date):
    cdef int i, j, closest_idx
    cdef float price_at_915, min_time_diff, time_diff
    cdef np.ndarray time_diffs
    cdef str symbol
    cdef object current_date_obj, target_time, batch_time  # Declare datetime variables as Python-level objects

    # Convert the current_date string to a datetime.date object
    current_date_obj = datetime.strptime(current_date, '%Y%m%d').date()

    # Calculate the target time
    target_time = datetime.combine(current_date_obj, datetime.strptime("09:15:00", '%H:%M:%S').time())

    # Get unique symbols
    unique_symbols = np.unique(original_df[:, 0])  # Assuming 'underly' is the first column

    for i in range(len(unique_symbols)):
        symbol = unique_symbols[i]
        batch_subset = batch_data_df[batch_data_df[:, 0] == symbol]  # Assuming 'Symbol' is the first column
        if len(batch_subset) == 0:
            continue

        min_time_diff = float('inf')
        closest_idx = -1

        for j in range(len(batch_subset)):
            batch_time = datetime.combine(current_date_obj, batch_subset[j, 1])  # Assuming 'Timestamp' is the second column
            time_diff = abs((batch_time - target_time).total_seconds())

            if time_diff < min_time_diff:
                min_time_diff = time_diff
                closest_idx = j

        price_at_915 = batch_subset[closest_idx, 2]  # Assuming 'Last_Trade' is the third column
        original_df[original_df[:, 0] == symbol, 1] = price_at_915  # Assuming 'price_at_915' is the second column

    return original_df