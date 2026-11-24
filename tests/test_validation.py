from datetime import datetime, timezone

def is_valid_event(event):
    required = ["event_id", "user_id", "movie_id", "rating", "event_ts"]
    if any(event.get(key) is None for key in required):
        return False
    try:
        rating = float(event["rating"])
        datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    return 0.5 <= rating <= 5.0

def test_valid_event():
    event = {"event_id":"e1","user_id":1,"movie_id":2,"rating":4.5,"event_ts":"2026-11-24T10:00:00Z"}
    assert is_valid_event(event)

def test_reject_out_of_range_rating():
    event = {"event_id":"e1","user_id":1,"movie_id":2,"rating":8,"event_ts":"2026-11-24T10:00:00Z"}
    assert not is_valid_event(event)

def test_reject_missing_event_id():
    event = {"user_id":1,"movie_id":2,"rating":4,"event_ts":"2026-11-24T10:00:00Z"}
    assert not is_valid_event(event)
