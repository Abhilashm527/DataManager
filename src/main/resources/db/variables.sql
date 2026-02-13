-- Create Variable Groups Table
CREATE TABLE IF NOT EXISTS dataloaders.variable_groups (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    application_id VARCHAR(36),
    environment VARCHAR(50),
    description TEXT,
    tags TEXT,
    group_color VARCHAR(10),
    created_at BIGINT,
    created_by VARCHAR(100),
    updated_at BIGINT,
    updated_by VARCHAR(100),
    deleted_at BIGINT,
    CONSTRAINT fk_variable_groups_application FOREIGN KEY (application_id) REFERENCES dataloaders.applications(id)
);

-- Create Variables Table
CREATE TABLE IF NOT EXISTS dataloaders.variables (
    id VARCHAR(36) PRIMARY KEY,
    group_id VARCHAR(36) NOT NULL,
    variable_key VARCHAR(100) NOT NULL,
    variable_value TEXT,
    is_secret BOOLEAN DEFAULT FALSE,
    description TEXT,
    created_at BIGINT,
    created_by VARCHAR(100),
    updated_at BIGINT,
    updated_by VARCHAR(100),
    deleted_at BIGINT,
    CONSTRAINT fk_variables_group FOREIGN KEY (group_id) REFERENCES dataloaders.variable_groups(id),
    CONSTRAINT unique_variable_key_per_group UNIQUE (group_id, variable_key)
);
