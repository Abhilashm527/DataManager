-- Add canvas_state column to dataflows table
ALTER TABLE dataloaders.dataflows ADD COLUMN canvas_state jsonb;
