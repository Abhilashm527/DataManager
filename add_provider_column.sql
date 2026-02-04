-- Add provider column to resource_types table
ALTER TABLE dataloaders.resource_types 
ADD COLUMN provider VARCHAR(50);

-- Add comment to the column
COMMENT ON COLUMN dataloaders.resource_types.provider IS 'Provider type: Connection, Source, or Target';

-- Optional: Update existing records with default values if needed
-- UPDATE dataloaders.resource_types SET provider = 'Connection' WHERE provider IS NULL;