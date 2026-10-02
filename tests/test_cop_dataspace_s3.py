from unittest.mock import MagicMock, patch

from eodc_eodag.collections.cop_dataspace_s3 import get_cop_dataspace_s3_quicklook_result


def _odata_response(assets):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"value": [{"Assets": assets}]}
    return resp


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_quicklook_result_fetches_the_quicklook_asset(mock_get, mock_aws):
    mock_get.return_value = _odata_response([
        {"Type": "MANIFEST", "S3Path": "/eodata/Sentinel-3/.../manifest.xml"},
        {"Type": "QUICKLOOK", "S3Path": "/eodata/Sentinel-3/.../quicklook.jpg"},
    ])
    mock_s3_client = MagicMock()
    mock_s3_client.get_object.return_value = {"Body": "fake-stream"}
    mock_aws.return_value = mock_s3_client

    stream, filename = get_cop_dataspace_s3_quicklook_result(item_id="S3A_OL_1_EFR____TEST")

    assert filename == "quicklook.jpg"
    assert stream == "fake-stream"
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="eodata", Key="Sentinel-3/.../quicklook.jpg"
    )


@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_quicklook_result_returns_none_when_no_quicklook(mock_get):
    mock_get.return_value = _odata_response([
        {"Type": "MANIFEST", "S3Path": "/eodata/Sentinel-3/.../manifest.xml"},
    ])

    assert get_cop_dataspace_s3_quicklook_result(item_id="S3A_OL_1_EFR____TEST") is None


@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_quicklook_result_returns_none_when_product_not_found(mock_get):
    mock_get.return_value = _odata_response(None)
    mock_get.return_value.json.return_value = {"value": []}

    assert get_cop_dataspace_s3_quicklook_result(item_id="S3A_OL_1_EFR____TEST") is None
