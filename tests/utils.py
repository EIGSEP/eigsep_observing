import numpy as np


def compare_dicts(dict1, dict2):
    """
    Compare two dictionaries for equality.
    """
    assert set(dict1) == set(dict2), "Dictionaries have different keys."
    for key in dict1:
        np.testing.assert_array_equal(
            dict1[key],
            dict2[key],
            err_msg=f"Arrays for key '{key}' are not equal.",
        )
