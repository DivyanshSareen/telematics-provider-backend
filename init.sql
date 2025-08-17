-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create the drivers table
CREATE TABLE IF NOT EXISTS drivers (
    driver_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    license_number VARCHAR(50) UNIQUE NOT NULL,
    phone_number VARCHAR(15),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create the vehicles table
CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    make VARCHAR(50) NOT NULL,
    model VARCHAR(50) NOT NULL,
    year INT,
    registration_number VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create the vehicle_locations table (time-series data)
CREATE TABLE IF NOT EXISTS vehicle_locations (
    time TIMESTAMPTZ NOT NULL,
    vehicle_id VARCHAR(50) NOT NULL REFERENCES vehicles(vehicle_id) ON DELETE RESTRICT,
    driver_id VARCHAR(50) NOT NULL REFERENCES drivers(driver_id) ON DELETE RESTRICT,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed FLOAT DEFAULT NULL,
    heading FLOAT DEFAULT NULL
);

-- Convert vehicle_locations to hypertable for time-series functionality
SELECT create_hypertable('vehicle_locations', 'time', if_not_exists => TRUE);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_vehicle_id ON vehicle_locations (vehicle_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_driver_id ON vehicle_locations (driver_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_location ON vehicle_locations (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_drivers_license ON drivers (license_number);
CREATE INDEX IF NOT EXISTS idx_vehicles_registration ON vehicles (registration_number);

-- Insert some sample data for testing
INSERT INTO drivers (driver_id, name, license_number, phone_number) VALUES
    ('DR001', 'John Doe', 'DL123456789', '+1234567890'),
    ('DR002', 'Jane Smith', 'DL987654321', '+1987654321'),
    ('DR003', 'Mike Johnson', 'DL456789123', '+1456789123')
ON CONFLICT (driver_id) DO NOTHING;

INSERT INTO vehicles (vehicle_id, make, model, year, registration_number) VALUES
    ('VH001', 'Toyota', 'Camry', 2022, 'ABC123'),
    ('VH002', 'Honda', 'Civic', 2021, 'XYZ789'),
    ('VH003', 'Ford', 'Focus', 2023, 'DEF456')
ON CONFLICT (vehicle_id) DO NOTHING;

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updating timestamps
CREATE TRIGGER update_drivers_updated_at BEFORE UPDATE ON drivers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_vehicles_updated_at BEFORE UPDATE ON vehicles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
