-- Deploy table DDL
CREATE TABLE IF NOT EXISTS dataloaders.deploys (
    deploy_id VARCHAR(255) PRIMARY KEY,
    deploy_name VARCHAR(255) NOT NULL UNIQUE,
    parent_id VARCHAR(255),
    job_id VARCHAR(255) NOT NULL,
    manual_run BOOLEAN DEFAULT FALSE,
    schedule BOOLEAN DEFAULT FALSE,
    schedule_expression VARCHAR(255),
    scheduler_id VARCHAR(255),
    active BOOLEAN DEFAULT FALSE,
    created_by VARCHAR(255),
    created_at BIGINT,
    updated_by VARCHAR(255),
    updated_at BIGINT,
    deleted_at BIGINT
);

-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_deploys_job_id ON dataloaders.deploys(job_id);
CREATE INDEX IF NOT EXISTS idx_deploys_parent_id ON dataloaders.deploys(parent_id);
CREATE INDEX IF NOT EXISTS idx_deploys_active ON dataloaders.deploys(active);
CREATE INDEX IF NOT EXISTS idx_deploys_deleted_at ON dataloaders.deploys(deleted_at);