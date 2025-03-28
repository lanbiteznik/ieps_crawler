# Set variables
$PGUser = "admin"
$DBName = "LanVitezBase"
$BackupFile = "C:\Users\turkf\Downloads\db_bak.sql"  # Update with actual path
$PostgresBin = "C:\Program Files\PostgreSQL\16\bin"  # Update PostgreSQL version if needed

# Add PostgreSQL binaries to PATH
$env:Path += ";$PostgresBin"

# Create the database
Write-Host "Creating database: $DBName"
& "$PostgresBin\createdb.exe" -U $PGUser $DBName

# Restore from SQL file
Write-Host "Restoring database from backup: $BackupFile"
& "$PostgresBin\psql.exe" -U $PGUser -d $DBName -f "$BackupFile"

Write-Host "Database restoration completed."