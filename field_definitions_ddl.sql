-- dataloaders.field_definitions definition

-- Drop table

-- DROP TABLE dataloaders.field_definitions;

CREATE TABLE dataloaders.field_definitions (
    field_definition_id varchar(20) NOT NULL,
    type_name varchar(100) NOT NULL,
    provider varchar(20) NULL,
    description text NULL,
    config_fields jsonb NULL,
    display_order int8 NULL DEFAULT 0,
    is_active bool NULL DEFAULT true,
    created_by varchar(100) NULL,
    created_at int8 NULL,
    updated_by varchar(100) NULL,
    updated_at int8 NULL,
    deleted_at int8 NULL,
    CONSTRAINT chk_provider CHECK (((provider)::text = ANY ((ARRAY['CONNECTION'::character varying, 'SOURCE'::character varying, 'TARGET'::character varying])::text[]))),
    CONSTRAINT field_definitions_pkey PRIMARY KEY (field_definition_id),
    CONSTRAINT unique_type_name_provider UNIQUE (type_name, provider)
);

CREATE INDEX idx_field_definitions_created_at ON dataloaders.field_definitions USING btree (created_at DESC);
CREATE INDEX idx_field_definitions_provider ON dataloaders.field_definitions USING btree (provider) WHERE (deleted_at IS NULL);
CREATE INDEX idx_field_definitions_is_active ON dataloaders.field_definitions USING btree (is_active) WHERE (deleted_at IS NULL);
CREATE INDEX idx_field_definitions_display_order ON dataloaders.field_definitions USING btree (display_order) WHERE (deleted_at IS NULL);