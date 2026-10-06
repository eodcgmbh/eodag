from unittest.mock import MagicMock, patch

import pytest

from eodc_eodag.collections.cop_dataspace_s3 import get_cop_dataspace_s3_asset_result, get_cop_dataspace_s3_folder_result


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

    # A .SAFE folder must never be treated as a single flat file here --
    # access() routes whole-product requests for folder-based items to
    # get_cop_dataspace_s3_folder_result() instead, before this is ever called.
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
def test_get_cop_dataspace_s3_folder_result_lists_every_real_file(mock_get, mock_aws):
    item_id = "S2A_MSIL2A_20240921T235231_N0511_R130_T55GFP_20240922T020450"
    safe_prefix = f"Sentinel-2/MSI/L2A/2024/09/21/{item_id}.SAFE"
    mock_get.return_value = _lookup_response(f"/eodata/{safe_prefix}")
    keys = [
        f"{safe_prefix}/MTD_MSIL2A.xml",
        f"{safe_prefix}/GRANULE/L2A_T55GFP_A048318_20240921T235633/IMG_DATA/R10m/T55GFP_20240921T235231_B04_10m.jp2",
    ]
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_aws.return_value = mock_s3

    result = get_cop_dataspace_s3_folder_result(item_id=item_id)

    assert result == [
        (keys[0], f"{item_id}.SAFE/MTD_MSIL2A.xml"),
        (keys[1], f"{item_id}.SAFE/GRANULE/L2A_T55GFP_A048318_20240921T235633/IMG_DATA/R10m/T55GFP_20240921T235231_B04_10m.jp2"),
    ]


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_folder_result_works_for_non_s2_missions(mock_get, mock_aws):
    # Mission-agnostic: no S2-specific regex involved -- e.g. a real ETAD item.
    item_id = "S1C_EW_ETA__AXDH_20260924T121736_20260924T121838_009589_013153_EFC8"
    safe_prefix = f"Sentinel-1/AUX/EW_ETA__AX/2026/09/24/{item_id}.SAFE"
    mock_get.return_value = _lookup_response(f"/eodata/{safe_prefix}")
    keys = [f"{safe_prefix}/manifest.safe", f"{safe_prefix}/measurement/{item_id}.nc"]
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_aws.return_value = mock_s3

    result = get_cop_dataspace_s3_folder_result(item_id=item_id)

    assert result == [
        (keys[0], f"{item_id}.SAFE/manifest.safe"),
        (keys[1], f"{item_id}.SAFE/measurement/{item_id}.nc"),
    ]


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_folder_result_returns_none_when_empty(mock_get, mock_aws):
    item_id = "S2A_MSIL2A_20240921T235231_N0511_R130_T55GFP_20240922T020450"
    mock_get.return_value = _lookup_response(f"/eodata/Sentinel-2/MSI/L2A/2024/09/21/{item_id}.SAFE")
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": []}]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_aws.return_value = mock_s3

    result = get_cop_dataspace_s3_folder_result(item_id=item_id)

    assert result is None


@patch("eodc_eodag.collections.cop_dataspace_s3.aws")
@patch("eodc_eodag.collections.cop_dataspace_s3.requests.get")
def test_get_cop_dataspace_s3_folder_result_raises_for_flat_file(mock_get, mock_aws):
    item_id = "S1D_OPER_AUX_PREORB_OPOD_20261006T064716_V20261006T060400_20261006T123900"
    mock_get.return_value = _lookup_response(f"/eodata/Sentinel-1/AUX/AUX_PREORB/2026/10/06/{item_id}.EOF")

    with pytest.raises(ValueError):
        get_cop_dataspace_s3_folder_result(item_id=item_id)


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
