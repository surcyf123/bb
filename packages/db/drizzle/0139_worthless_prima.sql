CREATE TABLE IF NOT EXISTS `client_turn_request_lookup` (
	`event_id` text PRIMARY KEY NOT NULL,
	`thread_id` text NOT NULL,
	`request_id` text NOT NULL,
	`sequence` integer NOT NULL,
	FOREIGN KEY (`event_id`) REFERENCES `events`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `client_turn_request_lookup_key_idx` ON `client_turn_request_lookup` (`thread_id`,`request_id`,`sequence`);