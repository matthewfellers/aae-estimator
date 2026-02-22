[README.md](https://github.com/user-attachments/files/25463784/README.md)
# AAE Automation — Panel Estimating System

## Files in this folder
- `app.py` — Main application (Flask web server + AI scan + calculation engine)
- `templates/index.html` — The web interface
- `requirements.txt` — Python packages needed
- `Procfile` — Tells Railway how to start the app
- `railway.toml` — Railway configuration
- `supabase_setup.sql` — Run this in Supabase to create the database table

## Deployment Steps

### Step 1 — Set up Supabase database
1. Go to supabase.com → your aae-estimator project
2. Click "SQL Editor" in the left sidebar
3. Paste the contents of `supabase_setup.sql`
4. Click "Run"
5. You should see "Success" — your bids table is ready

### Step 2 — Push code to GitHub
1. Go to github.com → New Repository → name it "aae-estimator" → Create
2. Follow GitHub's instructions to push this folder to the repo

### Step 3 — Connect Railway to GitHub
1. Go to railway.app → your project → your service
2. Click "Settings" → "Source" → Connect to GitHub repo "aae-estimator"
3. Railway will automatically deploy every time you push new code

### Step 4 — Verify environment variables in Railway
Make sure these are set in Railway → Variables:
- ANTHROPIC_API_KEY
- SUPABASE_URL  
- SUPABASE_ANON_KEY

### Step 5 — Get your URL
1. In Railway → your service → "Settings" → "Networking" → "Generate Domain"
2. Share that URL with your estimators and sales team

## That's it — the app is live.
