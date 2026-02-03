-- Activity Log table for tracking user activities

CREATE TABLE dataloaders.activity_logs (
	id varchar(50) NOT NULL,
	activity_type varchar(50) NOT NULL,
	entity_type varchar(50) NOT NULL,
	entity_id varchar(50) NOT NULL,
	entity_name varchar(255) NULL,
	action varchar(20) NOT NULL,
	description text NULL,
	user_id varchar(100) NOT NULL,
	created_at int8 NOT NULL,
	CONSTRAINT activity_logs_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_activity_logs_created_at ON dataloaders.activity_logs USING btree (created_at DESC);
CREATE INDEX idx_activity_logs_user_id ON dataloaders.activity_logs USING btree (user_id);
CREATE INDEX idx_activity_logs_entity_type ON dataloaders.activity_logs USING btree (entity_type);
CREATE INDEX idx_activity_logs_activity_type ON dataloaders.activity_logs USING btree (activity_type);