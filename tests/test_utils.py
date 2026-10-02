import io
import zipfile
from unittest.mock import MagicMock

import boto3.s3.transfer  # noqa: F401 -- registers boto3.s3.transfer as an
# attribute of the `boto3` module, same as eodag's own import chain does in
# production; without this, `boto3.s3.transfer.TransferConfig` (used by
# open_zip/stream_eodag_s3) raises AttributeError under plain `import boto3`.
import pytest

from eodc_eodag import utils


# ---------------------------------------------------------------------------
# _strip_archive_suffix
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "product_name,expected",
    [
        ("S2A_MSIL2A_X.SAFE", "S2A_MSIL2A_X"),
        ("S3A_SL_1_RBT____X.SEN3", "S3A_SL_1_RBT____X"),
        ("S5P_OFFL_L2__AUXDEM_X.nc", "S5P_OFFL_L2__AUXDEM_X.nc"),
        ("S1C_OPER_AUX_PREORB_X", "S1C_OPER_AUX_PREORB_X"),
    ],
)
def test_strip_archive_suffix(product_name, expected):
    assert utils._strip_archive_suffix(product_name) == expected


# ---------------------------------------------------------------------------
# open_zip
# ---------------------------------------------------------------------------

def _make_zip_bytes(names_and_contents):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in names_and_contents.items():
            zf.writestr(name, content)
    return buf.getvalue()


@pytest.fixture
def mock_s3():
    """A fake boto3 S3 client: download_file writes the zip to disk,
    upload_fileobj records every uploaded key."""
    s3 = MagicMock()
    uploaded = {}

    def fake_download_file(bucket, key, local_path):
        with open(local_path, "wb") as f:
            f.write(s3._zip_bytes)

    def fake_upload_fileobj(fileobj, Bucket, Key, **kwargs):
        uploaded[Key] = fileobj.read() if hasattr(fileobj, "read") else fileobj

    s3.download_file.side_effect = fake_download_file
    s3.upload_fileobj.side_effect = fake_upload_fileobj
    s3.uploaded = uploaded
    return s3


def test_open_zip_strips_archive_suffix_and_writes_completion_marker(mock_s3):
    mock_s3._zip_bytes = _make_zip_bytes({
        "PRODUCT.SAFE/manifest.safe": b"manifest",
        "PRODUCT.SAFE/measurement/B04.jp2": b"band data",
    })

    utils.open_zip(
        mock_s3,
        zip_product="some/key.zip",
        provider="cop_dataspace",
        collection="S3_SLSTR",
        item_id="PRODUCT.SAFE",
        s3_bucket="mybucket",
        target_provider="cop_dataspace_s3",
    )

    keys = set(mock_s3.uploaded.keys())
    assert "cop_dataspace_s3/S3_SLSTR/PRODUCT/manifest.safe" in keys
    assert "cop_dataspace_s3/S3_SLSTR/PRODUCT/measurement/B04.jp2" in keys
    assert "cop_dataspace_s3/S3_SLSTR/PRODUCT/_asset_mirror_complete" in keys
    assert mock_s3.uploaded["cop_dataspace_s3/S3_SLSTR/PRODUCT/_asset_mirror_complete"] == b""


def test_open_zip_keeps_s2_un_stripped_key_shape(mock_s3):
    mock_s3._zip_bytes = _make_zip_bytes({
        "S2_PRODUCT.SAFE/MTD_MSIL2A.xml": b"metadata",
    })

    utils.open_zip(
        mock_s3,
        zip_product="some/key.zip",
        provider="cop_dataspace",
        collection="S2_MSI_L2A",
        item_id="S2_PRODUCT",
        s3_bucket="mybucket",
        target_provider="cop_dataspace_s3",
    )

    keys = set(mock_s3.uploaded.keys())
    # S2 deliberately keeps the un-stripped internal zip path (see TODO in
    # open_zip), only the item_id-level prefix gets the suffix-stripped id.
    assert "cop_dataspace_s3/S2_MSI_L2A/S2_PRODUCT/S2_PRODUCT.SAFE/MTD_MSIL2A.xml" in keys
    assert "cop_dataspace_s3/S2_MSI_L2A/S2_PRODUCT/_asset_mirror_complete" in keys


# ---------------------------------------------------------------------------
# access() -- cop_dataspace_s3 routing
# ---------------------------------------------------------------------------

@pytest.fixture
def base_env(monkeypatch):
    monkeypatch.setenv("PROVIDER", "cop_dataspace_s3")
    monkeypatch.setenv("COLLECTION", "S3_SLSTR")
    monkeypatch.setenv("ITEM_ID", "S3A_SL_1_RBT____X")
    monkeypatch.setenv("PRODUCT_ID", "S1_radiance_an.nc")


def test_access_cop_dataspace_s3_prefers_whole_product_fetch(base_env, monkeypatch):
    fake_product = object()
    get_eodag_result = MagicMock(return_value=fake_product)
    stream_eodag_s3 = MagicMock(return_value="some/key.zip")
    open_zip = MagicMock()
    get_cop_dataspace_s3_asset_result = MagicMock()

    monkeypatch.setattr(utils, "get_eodag_result", get_eodag_result)
    monkeypatch.setattr(utils, "stream_eodag_s3", stream_eodag_s3)
    monkeypatch.setattr(utils, "open_zip", open_zip)
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_asset_result", get_cop_dataspace_s3_asset_result)

    utils.access(MagicMock(), s3_bucket="mybucket")

    get_eodag_result.assert_called_once_with(provider="cop_dataspace")
    stream_eodag_s3.assert_called_once()
    open_zip.assert_called_once()
    # The per-asset fallback must not be exercised when eodag resolves a product.
    get_cop_dataspace_s3_asset_result.assert_not_called()


def test_access_cop_dataspace_s3_falls_back_when_eodag_has_no_product_type(base_env, monkeypatch):
    get_eodag_result = MagicMock(return_value=None)
    stream_eodag_s3 = MagicMock()
    fake_stream = object()
    get_cop_dataspace_s3_asset_result = MagicMock(return_value=(fake_stream, "S1_radiance_an.nc"))
    stream_cop_dataspace_s3 = MagicMock()

    monkeypatch.setattr(utils, "get_eodag_result", get_eodag_result)
    monkeypatch.setattr(utils, "stream_eodag_s3", stream_eodag_s3)
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_asset_result", get_cop_dataspace_s3_asset_result)
    monkeypatch.setattr(utils, "stream_cop_dataspace_s3", stream_cop_dataspace_s3)

    utils.access(MagicMock(), s3_bucket="mybucket")

    get_eodag_result.assert_called_once_with(provider="cop_dataspace")
    stream_eodag_s3.assert_not_called()
    get_cop_dataspace_s3_asset_result.assert_called_once()
    assert stream_cop_dataspace_s3.call_count == 1
    _, args, kwargs = stream_cop_dataspace_s3.mock_calls[0]
    assert args[1] is fake_stream
    assert kwargs == {"S3_BUCKET": "mybucket", "real_key": "S1_radiance_an.nc"}


def test_access_cop_dataspace_s3_does_not_fail_on_bogus_asset(base_env, monkeypatch):
    monkeypatch.setattr(utils, "get_eodag_result", MagicMock(return_value=None))
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_asset_result", MagicMock(return_value=None))

    # Must complete cleanly (return), not raise -- a bogus/typo'd asset name
    # is a normal outcome, not an application error.
    utils.access(MagicMock(), s3_bucket="mybucket")
