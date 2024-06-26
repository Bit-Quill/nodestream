import pytest

from nodestream.pipeline.value_providers import NormalizerValueProvider
from nodestream.subclass_registry import MissingFromRegistryError

from ...stubs import StubbedValueProvider


def test_single_value(blank_context):
    subject = NormalizerValueProvider(
        using="lowercase_strings", data=StubbedValueProvider(["ABC"])
    )
    assert subject.single_value(blank_context) == "abc"


def test_many_values(blank_context):
    subject = NormalizerValueProvider(
        using="lowercase_strings", data=StubbedValueProvider(["ABC", "DEF"])
    )
    assert list(subject.many_values(blank_context)) == ["abc", "def"]


def test_invalid_normalizer(blank_context):
    with pytest.raises(MissingFromRegistryError):
        NormalizerValueProvider(
            using="not_a_normalizer", data=StubbedValueProvider(["ABC"])
        )
