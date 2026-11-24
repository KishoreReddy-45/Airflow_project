def deduplicate_by_event_id(rows):
    result = {}
    for row in rows:
        result[row["event_id"]] = row
    return list(result.values())

def test_deduplicate_event_id():
    rows = [
        {"event_id":"a", "rating":3.0},
        {"event_id":"a", "rating":4.0},
        {"event_id":"b", "rating":5.0},
    ]
    result = deduplicate_by_event_id(rows)
    assert len(result) == 2
    assert {r["event_id"] for r in result} == {"a", "b"}
