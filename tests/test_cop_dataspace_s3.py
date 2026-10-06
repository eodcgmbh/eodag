from unittest.mock import MagicMock, patch

from eodc_eodag.collections.cop_dataspace_s3 import get_cop_dataspace_s3_asset_result


def _lookup_response(s3path):
    resp = MagicMock()
    resp.json.return_value = {"value": [{"S3Path": s3path}]}
    return resp


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_whole_product_request_resolves_flat_file_regardless_of_asset_name(mock_get, mock_aws):
    item_id = "S2C_OPER_AUX_PROQUA_POD__20261005T103620_V20261003T235942_20261004T235941"
    mock_get.return_value = _lookup_response(f"/eodata/Sentinel-2/AUX/AUX_PROQUA/2026/10/03/{item_id}.TGZ")
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": "stream"}
    mock_aws.return_value = mock_s3

    stream, relative_path = get_cop_dataspace_s3_asset_result(product_id=item_id, item_id=item_id)

    assert relative_path == f"{item_id}.TGZ"
    mock_s3.get_object.assert_called_once_with(
        Bucket="eodata", Key=f"Sentinel-2/AUX/AUX_PROQUA/2026/10/03/{item_id}.TGZ"
    )


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_whole_product_request_does_not_misresolve_a_safe_folder_as_a_file(mock_get, mock_aws):
    item_id = "S1C_EW_ETA__AXDH_20260924T121736_20260924T121838_009589_013153_EFC8"
    mock_get.return_value = _lookup_response(f"/eodata/Sentinel-1/AUX/EW_ETA__AX/2026/09/24/{item_id}.SAFE")
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": f"Sentinel-1/AUX/EW_ETA__AX/2026/09/24/{item_id}.SAFE/manifest.safe"}]
    }
    mock_aws.return_value = mock_s3

    # A .SAFE folder must never be treated as a single flat file.
    result = get_cop_dataspace_s3_asset_result(product_id=item_id, item_id=item_id)

    assert result is None


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_explicit_asset_name_still_matches_by_extension(mock_get, mock_aws):
    item_id = "S1D_OPER_AUX_PREORB_OPOD_20261006T064716_V20261006T060400_20261006T123900"
    mock_get.return_value = _lookup_response(f"/eodata/Sentinel-1/AUX/AUX_PREORB/2026/10/06/{item_id}.EOF")
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": "stream"}
    mock_aws.return_value = mock_s3

    stream, relative_path = get_cop_dataspace_s3_asset_result(product_id=f"{item_id}_EOF", item_id=item_id)

    assert relative_path == f"{item_id}.EOF"


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_bogus_asset_name_returns_none(mock_get, mock_aws):
    item_id = "S1D_OPER_AUX_PREORB_OPOD_20261006T064716_V20261006T060400_20261006T123900"
    mock_get.return_value = _lookup_response(f"/eodata/Sentinel-1/AUX/AUX_PREORB/2026/10/06/{item_id}.EOF")
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": []}
    mock_aws.return_value = mock_s3

    result = get_cop_dataspace_s3_asset_result(product_id=f"{item_id}_bogus", item_id=item_id)

    assert result is None
