-- Migration: Add is_favorite column to dataloaders.applications
-- Run this script against your PostgreSQL database to support the favorite toggle feature.

ALTER TABLE dataloaders.applications
    ADD COLUMN IF NOT EXISTS is_favorite BOOLEAN NOT NULL DEFAULT FALSE;
