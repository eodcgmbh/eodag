import io
import zipfile
from unittest.mock import MagicMock

import boto3.s3.transfer  # noqa: F401 -- needed for boto3.s3.transfer.TransferConfig
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
    """Fake S3 client: download_file writes the zip, upload_fileobj records keys."""
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
    # S2 keeps the un-stripped internal zip path; only the prefix is stripped.
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


def test_access_cop_dataspace_s3_copies_non_zip_result_to_the_right_prefix(base_env, monkeypatch):
    """A non-zip single file (e.g. AUX orbit file) lands under
    cop_dataspace_s3, not the cop_dataspace prefix stream_eodag_s3 uploaded
    it to -- otherwise this backend's resolver never finds it."""
    fake_product = object()
    mock_s3 = MagicMock()
    monkeypatch.setattr(utils, "get_eodag_result", MagicMock(return_value=fake_product))
    monkeypatch.setattr(
        utils, "stream_eodag_s3",
        MagicMock(return_value="cop_dataspace/S1_AUX/S3A_SL_1_RBT____X/orbitfile.EOF"),
    )

    utils.access(mock_s3, s3_bucket="mybucket")

    mock_s3.copy_object.assert_called_once_with(
        Bucket="mybucket",
        CopySource={"Bucket": "mybucket", "Key": "cop_dataspace/S1_AUX/S3A_SL_1_RBT____X/orbitfile.EOF"},
        Key="cop_dataspace_s3/S3_SLSTR/S3A_SL_1_RBT____X/orbitfile.EOF",
    )


def test_access_cop_dataspace_whole_product_fallback_converges_on_cop_dataspace_s3(base_env, monkeypatch):
    """downloadLink's OData-direct fallback (no eodag registration, e.g. AUX)
    must write to the same prefix the named-asset path uses, or the two
    requests permanently diverge into two different copies of the file."""
    monkeypatch.setenv("PROVIDER", "cop_dataspace")
    fake_stream = object()
    stream_cop_dataspace_s3 = MagicMock()

    monkeypatch.setattr(utils, "get_eodag_result", MagicMock(return_value=None))
    monkeypatch.setattr(
        utils, "get_cop_dataspace_s3_whole_product_result",
        MagicMock(return_value=(fake_stream, "orbitfile.EOF")),
    )
    monkeypatch.setattr(utils, "stream_cop_dataspace_s3", stream_cop_dataspace_s3)

    utils.access(MagicMock(), s3_bucket="mybucket")

    stream_cop_dataspace_s3.assert_called_once()
    _, args, kwargs = stream_cop_dataspace_s3.mock_calls[0]
    assert args[1] is fake_stream
    assert kwargs == {"S3_BUCKET": "mybucket", "real_key": "orbitfile.EOF", "provider": "cop_dataspace_s3"}


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
    # Per-asset fallback must not run when eodag resolves a product.
    get_cop_dataspace_s3_asset_result.assert_not_called()


def test_access_cop_dataspace_s3_falls_back_when_eodag_has_no_product_type(base_env, monkeypatch):
    get_eodag_result = MagicMock(return_value=None)
    stream_eodag_s3 = MagicMock()
    fake_stream = object()
    get_cop_dataspace_s3_asset_result = MagicMock(return_value=(fake_stream, "S1_radiance_an.nc"))
    stream_cop_dataspace_s3 = MagicMock()

    monkeypatch.setattr(utils, "get_eodag_result", get_eodag_result)
    monkeypatch.setattr(utils, "stream_eodag_s3", stream_eodag_s3)
    # Flat file (not folder-based) -- _mirror_whole_folder must defer to the
    # per-asset resolver below.
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_folder_result", MagicMock(side_effect=ValueError))
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
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_folder_result", MagicMock(side_effect=ValueError))
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_asset_result", MagicMock(return_value=None))

    # A bogus asset name must return cleanly, not raise.
    utils.access(MagicMock(), s3_bucket="mybucket")


def test_access_cop_dataspace_s3_folder_fallback_uploads_every_file(base_env, monkeypatch):
    # Generalized whole-folder mirror: proven here against an ETAD item (no
    # S2-specific code path involved) rather than S2, which is the scenario
    # that used to be entirely unhandled (whole-product requests for a
    # folder-based item with no eodag product-type registration).
    item_id = "S1C_EW_ETA__AXDH_20260924T121736_20260924T121838_009589_013153_EFC8"
    monkeypatch.setenv("COLLECTION", "S1_AUX")
    monkeypatch.setenv("ITEM_ID", item_id)
    monkeypatch.setenv("PRODUCT_ID", item_id)

    files = [("eodata/key1", f"{item_id}.SAFE/manifest.safe"), ("eodata/key2", f"{item_id}.SAFE/measurement/{item_id}.nc")]
    contents = {real_key: f"content-{real_key}".encode() for real_key, _ in files}
    mock_s3_cdse = MagicMock()
    mock_s3_cdse.download_file.side_effect = lambda Bucket, Key, Filename: open(Filename, "wb").write(contents[Key])
    captured_streams = []
    stream_cop_dataspace_s3 = MagicMock(side_effect=lambda s3_arg, f, **kwargs: captured_streams.append((f.read(), kwargs)))
    uploaded = {}
    s3_eodc = MagicMock()
    s3_eodc.upload_fileobj.side_effect = lambda fileobj, Bucket, Key, **kwargs: uploaded.__setitem__(Key, fileobj.read())

    monkeypatch.setattr(utils, "get_eodag_result", MagicMock(return_value=None))
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_folder_result", MagicMock(return_value=files))
    monkeypatch.setattr(utils, "aws", MagicMock(return_value=mock_s3_cdse))
    monkeypatch.setattr(utils, "stream_cop_dataspace_s3", stream_cop_dataspace_s3)

    utils.access(s3_eodc, s3_bucket="mybucket")

    assert len(captured_streams) == 2
    for (content, kwargs), (real_key, relative_path) in zip(captured_streams, files):
        assert content == contents[real_key]
        assert kwargs == {
            "S3_BUCKET": "mybucket",
            "provider": "cop_dataspace_s3",
            "collection": "S1_AUX",
            "item_id": item_id,
            "real_key": relative_path,
        }

    assert f"cop_dataspace_s3/S1_AUX/{item_id}/_asset_mirror_complete" in uploaded

    import io
    import zipfile
    zip_key = f"cop_dataspace/S1_AUX/{item_id}/{item_id}.zip"
    assert zip_key in uploaded
    with zipfile.ZipFile(io.BytesIO(uploaded[zip_key])) as zf:
        assert sorted(zf.namelist()) == sorted(relative_path for _, relative_path in files)
        for real_key, relative_path in files:
            assert zf.read(relative_path) == contents[real_key]


def test_access_cop_dataspace_s3_folder_fallback_returns_cleanly_when_unresolvable(base_env, monkeypatch, capsys):
    monkeypatch.setenv("COLLECTION", "S2_MSI_L2A")
    monkeypatch.setenv("ITEM_ID", "S2A_MSIL2A_X")
    monkeypatch.setenv("PRODUCT_ID", "S2A_MSIL2A_X")

    monkeypatch.setattr(utils, "get_eodag_result", MagicMock(return_value=None))
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_folder_result", MagicMock(return_value=None))
    stream_cop_dataspace_s3 = MagicMock()
    monkeypatch.setattr(utils, "stream_cop_dataspace_s3", stream_cop_dataspace_s3)

    utils.access(MagicMock(), s3_bucket="mybucket")

    stream_cop_dataspace_s3.assert_not_called()
    assert "Uploaded product!" not in capsys.readouterr().out


def test_access_cop_dataspace_whole_product_fallback_mirrors_folder_for_safe_items(base_env, monkeypatch):
    # downloadLink (provider="cop_dataspace") for a folder-based item with no
    # eodag registration (e.g. ETAD) used to crash on the ValueError raised by
    # get_cop_dataspace_s3_whole_product_result() -- must mirror the folder instead.
    monkeypatch.setenv("PROVIDER", "cop_dataspace")
    monkeypatch.setenv("COLLECTION", "S1_AUX")
    item_id = "S1C_EW_ETA__AXDH_20260924T121736_20260924T121838_009589_013153_EFC8"
    monkeypatch.setenv("ITEM_ID", item_id)
    monkeypatch.setenv("PRODUCT_ID", f"{item_id}.zip")

    files = [("eodata/key1", f"{item_id}.SAFE/manifest.safe")]
    content = b"fake-manifest-bytes"
    mock_s3_cdse = MagicMock()
    mock_s3_cdse.download_file.side_effect = lambda Bucket, Key, Filename: open(Filename, "wb").write(content)
    captured_streams = []
    stream_cop_dataspace_s3 = MagicMock(side_effect=lambda s3_arg, f, **kwargs: captured_streams.append((f.read(), kwargs)))
    uploaded = {}
    s3_eodc = MagicMock()
    s3_eodc.upload_fileobj.side_effect = lambda fileobj, Bucket, Key, **kwargs: uploaded.__setitem__(Key, fileobj.read())

    monkeypatch.setattr(utils, "get_eodag_result", MagicMock(return_value=None))
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_whole_product_result", MagicMock(side_effect=ValueError))
    monkeypatch.setattr(utils, "get_cop_dataspace_s3_folder_result", MagicMock(return_value=files))
    monkeypatch.setattr(utils, "aws", MagicMock(return_value=mock_s3_cdse))
    monkeypatch.setattr(utils, "stream_cop_dataspace_s3", stream_cop_dataspace_s3)

    utils.access(s3_eodc, s3_bucket="mybucket")

    assert len(captured_streams) == 1
    content_read, kwargs = captured_streams[0]
    assert content_read == content
    assert kwargs == {
        "S3_BUCKET": "mybucket",
        "provider": "cop_dataspace_s3",
        "collection": "S1_AUX",
        "item_id": item_id,
        "real_key": f"{item_id}.SAFE/manifest.safe",
    }

    # downloadLink (provider="cop_dataspace") resolves this item's zip via a
    # plain head_object on this exact key -- must land here, not elsewhere.
    zip_key = f"cop_dataspace/S1_AUX/{item_id}/{item_id}.zip"
    assert zip_key in uploaded
