# Define paths to Conda, the Python script, and the logs directory
$condaBat = "C:\ProgramData\miniforge3\condabin\conda.bat"
$activateBat = "C:\ProgramData\miniforge3\condabin\activate.bat"
$envName = "duke_power_outages"
$scriptPath = ".\outages\scraper\counties.py"

# Create the logs directory if it doesn't exist
$logDir = ".\logs"
if (-not (Test-Path $logDir)) {
    New-Item -Path $logDir -ItemType Directory
}

# Generate a timestamp for the log filename
$timestamp = Get-Date -Format "yyyyMMddHHmmss"
$logFile = "$logDir/$timestamp.log"

# Step 1: Initialize Conda and activate the environment
& $condaBat "init" "powershell"  # Initialize Conda for PowerShell, if not done
& $condaBat activate $envName           # Activate the specified Conda environment

# Step 2: Run the Python script and redirect the output to the log file
python $scriptPath *>> $logFile

# Step 3: Optionally deactivate the environment
& $condaBat deactivate
