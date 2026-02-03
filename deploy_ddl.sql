-- dataloaders.resources definition

-- Drop table

-- DROP TABLE dataloaders.resources;

CREATE TABLE dataloaders.resources (
                                       resource_id varchar(50) NOT NULL,
                                       item_id varchar(64) NOT NULL,
                                       resource_type varchar(50) NOT NULL,
                                       resource_name varchar(255) NOT NULL,
                                       description text NULL,
                                       "configuration" jsonb NULL,
                                       status varchar(20) NOT NULL DEFAULT 'INACTIVE'::character varying,
                                       last_tested_at int8 NULL,
                                       is_active bool NOT NULL DEFAULT false,
                                       display_order int8 NOT NULL DEFAULT 0,
                                       created_by varchar(100) NOT NULL,
                                       created_at int8 NOT NULL,
                                       updated_by varchar(100) NULL,
                                       updated_at int8 NULL,
                                       deleted_at int8 NULL,
                                       CONSTRAINT chk_status CHECK (((status)::text = ANY ((ARRAY['ACTIVE'::character varying, 'INACTIVE'::character varying, 'FAILED'::character varying])::text[]))),
	CONSTRAINT resources_pkey PRIMARY KEY (resource_id)
);
CREATE INDEX idx_resources_created_at ON dataloaders.resources USING btree (created_at DESC);
CREATE INDEX idx_resources_item_id ON dataloaders.resources USING btree (item_id) WHERE (deleted_at IS NULL);
CREATE INDEX idx_resources_status ON dataloaders.resources USING btree (status) WHERE (deleted_at IS NULL);
CREATE INDEX idx_resources_type ON dataloaders.resources USING btree (resource_type) WHERE (deleted_at IS NULL);
CREATE UNIQUE INDEX unique_resource_name_per_item ON dataloaders.resources USING btree (item_id, resource_name) WHERE (deleted_at IS NULL);

-- dataloaders.resource_types definition

-- Drop table

-- DROP TABLE dataloaders.resource_types;

CREATE TABLE dataloaders.resource_types (
                                            resource_type_id varchar(20) NOT NULL,
                                            type_name varchar(100) NOT NULL,
                                            description text NULL,
                                            config_fields jsonb NULL,
                                            display_order int8 NULL DEFAULT 0,
                                            is_active bool NULL DEFAULT true,
                                            created_by varchar(100) NULL,
                                            created_at int8 NULL,
                                            updated_by varchar(100) NULL,
                                            updated_at int8 NULL,
                                            deleted_at int8 NULL,
                                            category varchar(20) NOT NULL DEFAULT 'RESOURCE'::character varying,
                                            CONSTRAINT chk_category CHECK (((category)::text = ANY ((ARRAY['RESOURCE'::character varying, 'SOURCE'::character varying, 'DESTINATION'::character varying])::text[]))),
	CONSTRAINT resource_types_pkey PRIMARY KEY (resource_type_id),
	CONSTRAINT unique_type_category UNIQUE (type_name, category)
);

-- dataloaders.mappings definition

-- Drop table

-- DROP TABLE dataloaders.mappings;

CREATE TABLE dataloaders.mappings (
                                      mapping_id varchar(255) NOT NULL,
                                      mapping_name varchar(500) NOT NULL,
                                      mappings jsonb NOT NULL DEFAULT '{}'::jsonb,
                                      is_active bool NULL DEFAULT false,
                                      item_id varchar(255) NOT NULL,
                                      created_by varchar(255) NOT NULL,
                                      created_at int8 NOT NULL,
                                      updated_by varchar(255) NULL,
                                      updated_at int8 NULL,
                                      deleted_at int8 NULL,
                                      CONSTRAINT mappings_pkey PRIMARY KEY (mapping_id),
                                      CONSTRAINT unique_mapping_name_per_item UNIQUE (mapping_name, item_id)
);


-- dataloaders.mappings foreign keys

ALTER TABLE dataloaders.mappings ADD CONSTRAINT fk_mappings_item_id FOREIGN KEY (item_id) REFERENCES dataloaders.items(id) ON DELETE CASCADE;


-- dataloaders.job_configs definition

-- Drop table

-- DROP TABLE dataloaders.job_configs;

CREATE TABLE dataloaders.job_configs (
                                         job_id varchar(50) NOT NULL,
                                         parent_job_id varchar(50) NULL,
                                         job_name varchar(255) NOT NULL,
                                         job_description text NULL,
                                         impacts varchar(500) NULL,
                                         job_severity varchar(20) NULL DEFAULT 'MEDIUM'::character varying,
                                         chunk_size int4 NULL DEFAULT 1000,
                                         mapping_id varchar(50) NULL,
                                         source_config jsonb NULL DEFAULT '{}'::jsonb,
                                         target_config jsonb NULL DEFAULT '{}'::jsonb,
                                         scheduled bool NULL DEFAULT false,
                                         schedule varchar(100) NULL,
                                         published bool NULL DEFAULT false,
                                         published_version varchar(20) NULL,
                                         deployed_version varchar(20) NULL,
                                         deployed bool NULL DEFAULT false,
                                         status varchar(20) NULL DEFAULT 'INACTIVE'::character varying,
                                         is_active bool NULL DEFAULT false,
                                         created_by varchar(100) NOT NULL,
                                         created_at int8 NOT NULL,
                                         updated_by varchar(100) NULL,
                                         updated_at int8 NULL,
                                         deleted_at int8 NULL,
                                         item_id varchar(50) NULL,
                                         drafted bool NULL DEFAULT false,
                                         CONSTRAINT job_configs_pkey PRIMARY KEY (job_id)
);
CREATE INDEX idx_job_configs_created_at ON dataloaders.job_configs USING btree (created_at);
CREATE INDEX idx_job_configs_deleted_at ON dataloaders.job_configs USING btree (deleted_at);
CREATE INDEX idx_job_configs_is_active ON dataloaders.job_configs USING btree (is_active);
CREATE INDEX idx_job_configs_item_id ON dataloaders.job_configs USING btree (item_id);
CREATE INDEX idx_job_configs_mapping_id ON dataloaders.job_configs USING btree (mapping_id);
CREATE INDEX idx_job_configs_parent_job_id ON dataloaders.job_configs USING btree (parent_job_id);
CREATE INDEX idx_job_configs_source_config ON dataloaders.job_configs USING gin (source_config);
CREATE INDEX idx_job_configs_status ON dataloaders.job_configs USING btree (status);
CREATE INDEX idx_job_configs_target_config ON dataloaders.job_configs USING gin (target_config);


-- dataloaders.job_configs foreign keys

ALTER TABLE dataloaders.job_configs ADD CONSTRAINT fk_job_configs_item_id FOREIGN KEY (item_id) REFERENCES dataloaders.items(id);
ALTER TABLE dataloaders.job_configs ADD CONSTRAINT fk_job_configs_mapping_id FOREIGN KEY (mapping_id) REFERENCES dataloaders.mappings(mapping_id);


-- dataloaders.job_config_references definition

-- Drop table

-- DROP TABLE dataloaders.job_config_references;

CREATE TABLE dataloaders.job_config_references (
                                                   id varchar(50) NOT NULL,
                                                   item_id varchar(50) NOT NULL,
                                                   job_id varchar(50) NOT NULL,
                                                   created_by varchar(100) NOT NULL,
                                                   created_at int8 NOT NULL,
                                                   updated_by varchar(100) NULL,
                                                   updated_at int8 NULL,
                                                   deleted_at int8 NULL,
                                                   CONSTRAINT job_config_references_pkey PRIMARY KEY (id),
                                                   CONSTRAINT unique_item_job UNIQUE (item_id, job_id)
);
CREATE INDEX idx_job_config_references_item_id ON dataloaders.job_config_references USING btree (item_id);
CREATE INDEX idx_job_config_references_job_id ON dataloaders.job_config_references USING btree (job_id);


-- dataloaders.job_config_references foreign keys

ALTER TABLE dataloaders.job_config_references ADD CONSTRAINT fk_job_config_references_item_id FOREIGN KEY (item_id) REFERENCES dataloaders.items(id);

-- dataloaders.items definition

-- Drop table

-- DROP TABLE dataloaders.items;

CREATE TABLE dataloaders.items (
                                   id varchar(64) NOT NULL,
                                   parent_id varchar(64) NULL,
                                   parent_folder_id varchar(64) NULL,
                                   "name" varchar(255) NULL,
                                   "type" varchar(100) NULL,
                                   item_type varchar(50) NULL,
                                   "path" varchar(500) NULL,
                                   item_reference varchar(255) NULL,
                                   deletable bool NULL DEFAULT true,
                                   active bool NULL DEFAULT true,
                                   root bool NULL DEFAULT false,
                                   "version" varchar(20) NULL DEFAULT '0.0.1'::character varying,
                                   created_by varchar(100) NULL,
                                   created_at int8 NULL,
                                   updated_by varchar(100) NULL,
                                   updated_at int8 NULL,
                                   deleted_at int8 NULL,
                                   CONSTRAINT items_pkey PRIMARY KEY (id)
);

-- dataloaders.deploys definition

-- Drop table

-- DROP TABLE dataloaders.deploys;

CREATE TABLE dataloaders.deploys (
                                     deploy_id varchar(255) NOT NULL,
                                     deploy_name varchar(255) NOT NULL,
                                     parent_id varchar(255) NULL,
                                     job_id varchar(255) NOT NULL,
                                     manual_run bool NULL DEFAULT false,
                                     schedule bool NULL DEFAULT false,
                                     schedule_expression varchar(255) NULL,
                                     active bool NULL DEFAULT false,
                                     created_by varchar(255) NULL,
                                     created_at int8 NULL,
                                     updated_by varchar(255) NULL,
                                     updated_at int8 NULL,
                                     deleted_at int8 NULL,
                                     scheduler_name varchar(500) NULL,
                                     scheduler_id int8 NULL,
                                     CONSTRAINT deploys_deploy_name_key UNIQUE (deploy_name),
                                     CONSTRAINT deploys_pkey PRIMARY KEY (deploy_id)
);
CREATE INDEX idx_deploys_active ON dataloaders.deploys USING btree (active);
CREATE INDEX idx_deploys_deleted_at ON dataloaders.deploys USING btree (deleted_at);
CREATE INDEX idx_deploys_job_id ON dataloaders.deploys USING btree (job_id);
CREATE INDEX idx_deploys_parent_id ON dataloaders.deploys USING btree (parent_id);

-- dataloaders.datatable definition

-- Drop table

-- DROP TABLE dataloaders.datatable;

CREATE TABLE dataloaders.datatable (
                                       id varchar(64) NOT NULL,
                                       datatable_id varchar(64) NOT NULL,
                                       application_id varchar(64) NULL,
                                       deleted_at int8 NULL,
                                       created_at int8 NULL,
                                       created_by varchar(100) NULL,
                                       updated_at int8 NULL,
                                       updated_by varchar(100) NULL,
                                       CONSTRAINT datatable_pkey PRIMARY KEY (id)
);


-- dataloaders.datatable foreign keys

ALTER TABLE dataloaders.datatable ADD CONSTRAINT fk_datatable_application FOREIGN KEY (application_id) REFERENCES dataloaders.items(id) ON DELETE CASCADE;

-- dataloaders.activity_logs definition

-- Drop table

-- DROP TABLE dataloaders.activity_logs;

CREATE TABLE dataloaders.activity_logs (
                                           id varchar(50) NOT NULL,
                                           activity_type varchar(50) NOT NULL,
                                           entity_type varchar(50) NOT NULL,
                                           entity_id varchar(50) NOT NULL,
                                           entity_name varchar(255) NULL,
                                           "action" varchar(200) NOT NULL,
                                           description text NULL,
                                           user_id varchar(100) NOT NULL,
                                           created_at int8 NOT NULL,
                                           CONSTRAINT activity_logs_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_activity_logs_activity_type ON dataloaders.activity_logs USING btree (activity_type);
CREATE INDEX idx_activity_logs_created_at ON dataloaders.activity_logs USING btree (created_at DESC);
CREATE INDEX idx_activity_logs_entity_type ON dataloaders.activity_logs USING btree (entity_type);
CREATE INDEX idx_activity_logs_user_id ON dataloaders.activity_logs USING btree (user_id);