# powershell
#
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
#
# .\bloom_filter_test.ps1

# --- Bloom Filter Configuration ---
$ExpectedItems = 100000
$FalsePositiveRate = 0.05
# Formula for optimal bit array size: m = -n*ln(p) / (ln(2)^2)
$BitSize = 700000 
$Bits = New-Object System.Collections.BitArray($BitSize)

# --- Function: Add to Filter ---
function Add-ToFilter([string]$Item) {
    $h1 = [Math]::Abs($Item.GetHashCode() % $BitSize)
    $h2 = [Math]::Abs(($Item + "seed2").GetHashCode() % $BitSize)
    
    $Bits.Set($h1, $true)
    $Bits.Set($h2, $true)
}

# --- Function: Test Filter ---
function Test-Filter([string]$Item) {
    $h1 = [Math]::Abs($Item.GetHashCode() % $BitSize)
    $h2 = [Math]::Abs(($Item + "seed2").GetHashCode() % $BitSize)
    
    if ($Bits.Get($h1) -and $Bits.Get($h2)) {
        return $true # Possibly in set
    }
    return $false # Definitely not
}

# --- Demonstration ---
Write-Host "Adding 100,000 Kings and items..." -ForegroundColor Cyan
foreach ($i in 1..100000) {
    Add-ToFilter "King_$i"
}

# Test the filter
$testName = "King_555"
if (Test-Filter $testName) {
    Write-Host "[Result] $testName is PROBABLY in the set." -ForegroundColor Green
}

$fakeName = "Shallum_The_Usurper"
if (-not (Test-Filter $fakeName)) {
    Write-Host "[Result] $fakeName is DEFINITELY NOT in the set." -ForegroundColor Red
}