CREATE INDEX IF NOT EXISTS `events_client_turn_request_id_idx` ON `events` (`thread_id`,json_extract("data", '$.requestId')) WHERE "events"."type" = 'client/turn/requested';
