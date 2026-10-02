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
    # The Assets entry's own S3Path is just the product's directory (same as
    # the main product's) -- real behavior, confirmed against production.
    product_dir = "/eodata/Sentinel-3/.../S3A_PRODUCT.SEN3"
    mock_get.return_value = _odata_response([
        {"Type": "MANIFEST", "S3Path": product_dir},
        {"Type": "QUICKLOOK", "S3Path": product_dir},
    ])
    mock_s3_client = MagicMock()
    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "Sentinel-3/.../S3A_PRODUCT.SEN3/xfdumanifest.xml"},
            {"Key": "Sentinel-3/.../S3A_PRODUCT.SEN3/quicklook.jpg"},
            {"Key": "Sentinel-3/.../S3A_PRODUCT.SEN3/geo_coordinates.nc"},
        ]
    }
    mock_s3_client.get_object.return_value = {"Body": "fake-stream"}
    mock_aws.return_value = mock_s3_client

    stream, filename = get_cop_dataspace_s3_quicklook_result(
        product_id="quicklook.jpg", item_id="S3A_OL_1_EFR____TEST"
    )

    assert filename == "quicklook.jpg"
    assert stream == "fake-stream"
    mock_s3_client.list_objects_v2.assert_called_once_with(
        Bucket="eodata", Prefix="Sentinel-3/.../S3A_PRODUCT.SEN3/", MaxKeys=1000
    )
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="eodata", Key="Sentinel-3/.../S3A_PRODUCT.SEN3/quicklook.jpg"
    )


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_quicklook_result_picks_the_exact_requested_name(mock_get, mock_aws):
    # Regression: S1 SAR products have TWO real thumbnail-looking files in
    # the same directory (quick-look.png and thumbnail.png) -- matching "any
    # thumbnail-looking name" would risk silently serving the wrong one.
    product_dir = "/eodata/Sentinel-1/SAR/.../PRODUCT.SAFE"
    mock_get.return_value = _odata_response([
        {"Type": "QUICKLOOK", "S3Path": product_dir},
    ])
    mock_s3_client = MagicMock()
    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "Sentinel-1/SAR/.../PRODUCT.SAFE/quick-look.png"},
            {"Key": "Sentinel-1/SAR/.../PRODUCT.SAFE/thumbnail.png"},
        ]
    }
    mock_s3_client.get_object.return_value = {"Body": "fake-stream"}
    mock_aws.return_value = mock_s3_client

    stream, filename = get_cop_dataspace_s3_quicklook_result(
        product_id="thumbnail.png", item_id="S1_TEST"
    )

    assert filename == "thumbnail.png"
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="eodata", Key="Sentinel-1/SAR/.../PRODUCT.SAFE/thumbnail.png"
    )


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_quicklook_result_returns_none_when_no_file_matches(mock_get, mock_aws):
    mock_get.return_value = _odata_response([
        {"Type": "QUICKLOOK", "S3Path": "/eodata/Sentinel-3/.../S3A_PRODUCT.SEN3"},
    ])
    mock_s3_client = MagicMock()
    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "Sentinel-3/.../S3A_PRODUCT.SEN3/xfdumanifest.xml"}]
    }
    mock_aws.return_value = mock_s3_client

    assert get_cop_dataspace_s3_quicklook_result(
        product_id="quicklook.jpg", item_id="S3A_OL_1_EFR____TEST"
    ) is None


@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_quicklook_result_returns_none_when_no_quicklook(mock_get):
    mock_get.return_value = _odata_response([
        {"Type": "MANIFEST", "S3Path": "/eodata/Sentinel-3/.../manifest.xml"},
    ])

    assert get_cop_dataspace_s3_quicklook_result(
        product_id="quicklook.jpg", item_id="S3A_OL_1_EFR____TEST"
    ) is None


@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_quicklook_result_returns_none_when_product_not_found(mock_get):
    mock_get.return_value = _odata_response(None)
    mock_get.return_value.json.return_value = {"value": []}

    assert get_cop_dataspace_s3_quicklook_result(
        product_id="quicklook.jpg", item_id="S3A_OL_1_EFR____TEST"
    ) is None
