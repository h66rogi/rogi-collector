CREATE TABLE IF NOT EXISTS collector_runtime_marker(
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  created_at timestamptz NOT NULL DEFAULT now()
);
