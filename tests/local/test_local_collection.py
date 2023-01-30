from openeo.local import LocalConnection

def test_local_collection_metadata(create_local_netcdf):
    local_conn = LocalConnection(create_local_netcdf.as_posix())
    assert len(local_conn.list_collections()) == 1
