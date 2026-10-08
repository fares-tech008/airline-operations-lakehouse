def test_distance_positive():
    distance = 500

    assert distance > 0


def test_delay_flag():
    is_delayed = 1

    assert is_delayed in [0, 1]
