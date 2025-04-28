from .C import all_create_tests
from .D import all_delete_tests
from .R import all_read_tests
from .U import all_update_tests

all_tests = [
    *all_read_tests,
    *all_update_tests,
    *all_create_tests,
    *all_delete_tests
]
