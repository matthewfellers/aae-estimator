-- Upgrade shipping_sticker_library: single folder → multiple folders
-- Run in Supabase SQL Editor

-- Step 1: Add the new array column
ALTER TABLE shipping_sticker_library
ADD COLUMN IF NOT EXISTS folders text[] NOT NULL DEFAULT '{}';

-- Step 2: Migrate existing folder data (if the old column exists)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'shipping_sticker_library' AND column_name = 'folder'
  ) THEN
    UPDATE shipping_sticker_library
    SET folders = ARRAY[folder]
    WHERE folder IS NOT NULL AND folder != '';

    ALTER TABLE shipping_sticker_library DROP COLUMN folder;
  END IF;
END $$;

-- Step 3: Index for fast folder lookups at scale
CREATE INDEX IF NOT EXISTS idx_sticker_library_folders
ON shipping_sticker_library USING GIN (folders);
