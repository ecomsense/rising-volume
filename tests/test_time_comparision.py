import pendulum as pdlm
import time


def compare_types(pdlm_time: float, time_time: float):
    """Test to compare pendulum and time types"""
    assert isinstance(
        pdlm_time, float
    ), f"pdlm_time is {type(pdlm_time)}, expected float"
    assert isinstance(
        time_time, float
    ), f"time_time is {type(time_time)}, expected float"
    assert type(pdlm_time) is type(time_time), "Types do not match"


pdlm_time = pdlm.now().timestamp()
time_time = time.time()

print(f"{time_time=} and {pdlm_time=}")
compare_types(pdlm_time, time_time)
