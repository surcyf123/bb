CREATE TRIGGER IF NOT EXISTS client_request_lookup_insert
AFTER INSERT ON events
WHEN NEW.type = 'client/turn/requested'
BEGIN
  INSERT OR REPLACE INTO client_turn_request_lookup(event_id,thread_id,request_id,sequence)
  SELECT NEW.id,NEW.thread_id,json_extract(NEW.data,'$.requestId'),NEW.sequence
  WHERE CASE WHEN json_valid(NEW.data) THEN json_type(NEW.data,'$.requestId') END = 'text';
END;
--> statement-breakpoint
CREATE TRIGGER IF NOT EXISTS client_request_lookup_update
AFTER UPDATE OF id,thread_id,type,data,sequence ON events
WHEN OLD.type = 'client/turn/requested' OR NEW.type = 'client/turn/requested'
BEGIN
  DELETE FROM client_turn_request_lookup WHERE event_id = OLD.id;
  INSERT OR REPLACE INTO client_turn_request_lookup(event_id,thread_id,request_id,sequence)
  SELECT NEW.id,NEW.thread_id,json_extract(NEW.data,'$.requestId'),NEW.sequence
  WHERE NEW.type = 'client/turn/requested'
    AND CASE WHEN json_valid(NEW.data) THEN json_type(NEW.data,'$.requestId') END = 'text';
END;
--> statement-breakpoint
CREATE TRIGGER IF NOT EXISTS client_request_lookup_delete
AFTER DELETE ON events
WHEN OLD.type = 'client/turn/requested'
BEGIN
  DELETE FROM client_turn_request_lookup WHERE event_id = OLD.id;
END;
--> statement-breakpoint
INSERT OR IGNORE INTO client_turn_request_lookup(event_id,thread_id,request_id,sequence)
SELECT id,thread_id,json_extract(data,'$.requestId'),sequence
FROM events
WHERE type = 'client/turn/requested'
  AND CASE WHEN json_valid(data) THEN json_type(data,'$.requestId') END = 'text';
