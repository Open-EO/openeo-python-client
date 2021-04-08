from openeo.udf import StructuredData


def test_structured_data_list_basic():
    values = [2, 3, 5, 8]
    st = StructuredData(data=values)
    assert st.to_dict() == {"data": [2, 3, 5, 8], "type": "list", "description": "list"}


def test_structured_data_list_full():
    values = [2, 3, 5, 8]
    st = StructuredData(data=values, type="mylist", description="A list")
    assert st.to_dict() == {"data": [2, 3, 5, 8], "type": "mylist", "description": "A list"}


def test_structured_data_dict_basic():
    values = {"a": 1, "b": 3}
    st = StructuredData(data=values)
    assert st.to_dict() == {"data": {"a": 1, "b": 3}, "type": "dict", "description": "dict"}


def test_structured_data_dict_full():
    values = {"a": 1, "b": 3}
    st = StructuredData(data=values, type="mydict", description="Key-value output")
    assert st.to_dict() == {"data": {"a": 1, "b": 3}, "type": "mydict", "description": "Key-value output"}


def test_structured_data_table():
    values = [("col_1", "col_2"), (1, 2), (2, 3)]
    st = StructuredData(data=values, type="table", description="Table output")
    assert st.to_dict() == {
        "data": [("col_1", "col_2"), (1, 2), (2, 3)], "type": "table", "description": "Table output"
    }
