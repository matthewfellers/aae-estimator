-- Add folder column to shipping_sticker_library
-- Run in Supabase SQL Editor

ALTER TABLE shipping_sticker_library
ADD COLUMN IF NOT EXISTS folder text NOT NULL DEFAULT '';
