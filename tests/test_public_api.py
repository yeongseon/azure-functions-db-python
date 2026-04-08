from azure_functions_db import __version__


def test_version() -> None:
    assert __version__ == "0.1.0"


def test_public_api() -> None:
    from azure_functions_db import __all__

    assert "__version__" in __all__
