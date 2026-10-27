CREATE TABLE `bb_local_child_notification_outbox` (
	`source_key` text PRIMARY KEY NOT NULL,
	`parent_thread_id` text NOT NULL,
	`child_thread_id` text NOT NULL,
	`turn_id` text,
	`data` text NOT NULL,
	`interruption_data` text,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`parent_thread_id`) REFERENCES `threads`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`child_thread_id`) REFERENCES `threads`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `bb_local_child_notification_outbox_created_idx` ON `bb_local_child_notification_outbox` (`created_at`);--> statement-breakpoint
CREATE TABLE `bb_local_child_notification_receipts` (
	`source_key` text PRIMARY KEY NOT NULL,
	`parent_thread_id` text NOT NULL,
	`child_thread_id` text NOT NULL,
	`queued_message_id` text NOT NULL,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`parent_thread_id`) REFERENCES `threads`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `bb_local_child_notification_receipts_parent_created_idx` ON `bb_local_child_notification_receipts` (`parent_thread_id`,`created_at`);--> statement-breakpoint
CREATE INDEX `bb_local_child_notification_receipts_queue_idx` ON `bb_local_child_notification_receipts` (`queued_message_id`);--> statement-breakpoint
ALTER TABLE `queued_thread_messages` ADD `bb_local_originally_scheduled_at` integer;
--> statement-breakpoint
UPDATE `queued_thread_messages` SET `bb_local_originally_scheduled_at` = `send_at` WHERE `send_at` IS NOT NULL;
