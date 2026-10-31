from collections.abc import Iterator

import pytest
from blockbuster import BlockBuster, blockbuster_ctx


@pytest.fixture(autouse=True)
def blocking_calls() -> Iterator[BlockBuster]:
    with blockbuster_ctx() as detector:
        yield detector
