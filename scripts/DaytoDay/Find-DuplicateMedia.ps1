#Requires -Version 5.1
<#
.SYNOPSIS
    Scans D:\ for duplicate media files, reports them, and optionally deletes duplicates.

.DESCRIPTION
    Phase 1  : Recursively scans D:\ for media files, groups by size as a pre-filter,
               computes SHA256 hashes, and writes D:\DuplicateReport.csv.
               The file with the shortest path in each duplicate group is kept as
               ORIGINAL; all others are marked DUPLICATE.

    Phase 2  : Activated by -Delete. Reads in-memory results (or an existing CSV via
               -ReportPath), prompts for confirmation, deletes DUPLICATE files, and
               appends a record to D:\DeletionLog.csv.
               Pass -WhatIf for a no-op dry-run.

.PARAMETER Delete
    Activate Phase 2: delete files marked DUPLICATE after user confirmation.

.PARAMETER ReportPath
    Path to an existing DuplicateReport.csv. When used with -Delete, skips Phase 1
    and reads this file instead of re-scanning.

.EXAMPLE
    .\Find-DuplicateMedia.ps1
    Phase 1 only -- scan D:\ and write the report.

.EXAMPLE
    .\Find-DuplicateMedia.ps1 -Delete
    Scan then prompt to delete duplicates.

.EXAMPLE
    .\Find-DuplicateMedia.ps1 -Delete -WhatIf
    Dry-run: show what would be deleted without touching any files.

.EXAMPLE
    .\Find-DuplicateMedia.ps1 -Delete -ReportPath "D:\DuplicateReport.csv"
    Delete using an existing report -- no re-scan needed.
#>

[CmdletBinding(SupportsShouldProcess)]   # Adds -WhatIf and -Confirm as standard switches
param(
    [switch]$Delete,
    [string]$ReportPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

# -----------------------------------------------------------------------------
# CONFIGURATION -- edit these if needed
# -----------------------------------------------------------------------------

$ScanRoot        = 'D:\'
$ReportCsvPath   = 'D:\DuplicateReport.csv'
$DeletionLogPath = 'D:\DeletionLog.csv'
$ErrorLogPath    = 'D:\ScanErrors.log'

# Directories to skip entirely (case-insensitive prefix match)
$ExcludedDirs = @(
    'D:\Program Files',
    'D:\Program Files (x86)'
)

# Media file extensions to target (lowercase)
$TargetExtensions = New-Object 'System.Collections.Generic.HashSet[string]' `
    ([System.StringComparer]::OrdinalIgnoreCase)
@(
    '.mp3', '.mp4', '.avi', '.mkv', '.mov', '.wmv',
    '.flac', '.wav', '.aac', '.ogg', '.wma',
    '.jpg', '.jpeg', '.png', '.gif', '.bmp',
    '.tiff', '.webp', '.svg', '.heic',
    '.webm', '.m4a', '.m4v'
) | ForEach-Object { [void]$TargetExtensions.Add($_) }

# Maximum duplicate groups printed to the console (full list is always in the CSV)
$MaxConsoleGroups = 30

# -----------------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------------

# Append a timestamped entry to the error/warning log file
function Write-Log {
    param(
        [string]$Message,
        [ValidateSet('INFO','WARN','ERROR')]
        [string]$Level = 'INFO'
    )
    $ts    = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $entry = "[$ts] [$Level] $Message"
    Add-Content -Path $ErrorLogPath -Value $entry -Encoding UTF8
}

# Convert a byte count into a human-readable string (B / KB / MB / GB)
function Format-FileSize {
    param([long]$Bytes)
    if ($Bytes -ge 1GB) { return ('{0:N2} GB' -f ($Bytes / 1GB)) }
    if ($Bytes -ge 1MB) { return ('{0:N2} MB' -f ($Bytes / 1MB)) }
    if ($Bytes -ge 1KB) { return ('{0:N2} KB' -f ($Bytes / 1KB)) }
    return "$Bytes B"
}

# Compute the SHA256 hash of a file; returns $null on error and logs the failure
function Get-FileHashSafe {
    param([string]$FilePath)
    try {
        return (Get-FileHash -Path $FilePath -Algorithm SHA256 -ErrorAction Stop).Hash
    }
    catch {
        Write-Log -Message "Hash failed: $FilePath -- $($_.Exception.Message)" -Level ERROR
        return $null
    }
}

# Return $true if a file path falls inside one of the excluded directories
function Test-IsExcluded {
    param([string]$FilePath)
    foreach ($dir in $ExcludedDirs) {
        # Append a backslash so "D:\Program Files" does NOT match "D:\Program Files (x86)"
        $prefix = $dir.TrimEnd('\') + '\'
        if ($FilePath.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            return $true
        }
    }
    return $false
}

# -----------------------------------------------------------------------------
# PHASE 1 -- DISCOVERY & REPORT
# -----------------------------------------------------------------------------

function Invoke-DuplicateScan {

    Write-Host ''
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host '  PHASE 1 -- DUPLICATE MEDIA SCANNER' -ForegroundColor Cyan
    Write-Host "  Root : $ScanRoot" -ForegroundColor Cyan
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host ''

    # ------------------------------------------------------------------
    # STEP 1 -- Enumerate every matching media file under $ScanRoot
    # ------------------------------------------------------------------
    Write-Host '[1/4] Enumerating media files...' -ForegroundColor Yellow

    $allFiles   = New-Object 'System.Collections.Generic.List[System.IO.FileInfo]'
    $enumTotal  = 0
    $enumErrors = @()   # populated by -ErrorVariable below

    Get-ChildItem -LiteralPath $ScanRoot -Recurse -File `
                  -ErrorAction SilentlyContinue `
                  -ErrorVariable +enumErrors |
    ForEach-Object {
        $enumTotal++

        # Refresh the progress bar every 500 files to avoid UI overhead on large drives
        if ($enumTotal % 500 -eq 0) {
            Write-Progress -Activity 'Enumerating files' `
                           -Status "$($enumTotal) files scanned, $($allFiles.Count) media found" `
                           -PercentComplete -1
        }

        # Drop files inside excluded directories
        if (Test-IsExcluded -FilePath $_.FullName) { return }

        # Drop files whose extension is not in the target set
        if (-not $TargetExtensions.Contains($_.Extension.ToLower())) { return }

        $allFiles.Add($_)
    }

    Write-Progress -Activity 'Enumerating files' -Completed

    # Log any access/permission errors encountered during enumeration
    foreach ($err in $enumErrors) {
        $target = if ($err.TargetObject) { $err.TargetObject } else { 'unknown path' }
        Write-Log -Message "Enumeration skipped: $target -- $($err.Exception.Message)" -Level WARN
    }

    $mediaCount = $allFiles.Count
    Write-Host "  Found $($mediaCount) media files ($($enumTotal) total files scanned)" `
               -ForegroundColor Green
    Write-Host ''

    # ------------------------------------------------------------------
    # STEP 2 -- Group by file size (cheap pre-filter)
    #           Only files that share a size with at least one other file
    #           can possibly be duplicates -- no point hashing the rest.
    # ------------------------------------------------------------------
    Write-Host '[2/4] Grouping files by size (pre-filter)...' -ForegroundColor Yellow

    $sizeGroups     = $allFiles | Group-Object -Property Length | Where-Object { $_.Count -gt 1 }
    $candidates     = @($sizeGroups | ForEach-Object { $_.Group })
    $candidateCount = $candidates.Count
    $groupCount     = @($sizeGroups).Count

    Write-Host "  $($groupCount) size groups with potential duplicates ($($candidateCount) files to hash)" `
               -ForegroundColor Green
    Write-Host ''

    if ($candidateCount -eq 0) {
        Write-Host '  No candidate duplicates found. Exiting.' -ForegroundColor Green
        return New-Object 'System.Collections.Generic.List[PSCustomObject]'
    }

    # ------------------------------------------------------------------
    # STEP 3 -- Compute SHA256 for each candidate file
    #           Build a hash-to-FileInfo-list lookup table.
    # ------------------------------------------------------------------
    Write-Host '[3/4] Computing SHA256 hashes...' -ForegroundColor Yellow

    $hashMap   = @{}   # hash string -> List<FileInfo>
    $doneCount = 0

    foreach ($file in $candidates) {
        $doneCount++
        $pct = [int](($doneCount / $candidateCount) * 100)

        Write-Progress -Activity 'Computing SHA256 hashes' `
                       -Status "[$($doneCount) / $($candidateCount)]  $($file.Name)" `
                       -PercentComplete $pct `
                       -CurrentOperation $file.FullName

        $hash = Get-FileHashSafe -FilePath $file.FullName
        if ($null -eq $hash) { continue }   # already logged inside Get-FileHashSafe

        if (-not $hashMap.ContainsKey($hash)) {
            $hashMap[$hash] = New-Object 'System.Collections.Generic.List[System.IO.FileInfo]'
        }
        $hashMap[$hash].Add($file)
    }

    Write-Progress -Activity 'Computing SHA256 hashes' -Completed

    # Keep only groups that truly have more than one file (actual duplicates)
    $dupGroups    = @($hashMap.GetEnumerator() | Where-Object { $_.Value.Count -gt 1 })
    $dupGroupCount = $dupGroups.Count

    Write-Host "  $($dupGroupCount) duplicate hash group(s) found" -ForegroundColor Green
    Write-Host ''

    # ------------------------------------------------------------------
    # STEP 4 -- Build the report rows
    #           Within each duplicate group, the file with the shortest
    #           full path is designated ORIGINAL (most "canonical").
    #           All others are marked DUPLICATE.
    # ------------------------------------------------------------------
    Write-Host '[4/4] Building report...' -ForegroundColor Yellow

    $reportRows        = New-Object 'System.Collections.Generic.List[PSCustomObject]'
    $totalDupeFiles    = 0
    $totalReclaimBytes = [long]0

    foreach ($group in $dupGroups) {
        $hash  = $group.Key
        # Sort ascending by path length so index 0 = shortest path = ORIGINAL
        $files = $group.Value | Sort-Object { $_.FullName.Length }

        $isFirst = $true
        foreach ($file in $files) {
            if ($isFirst) {
                $status = 'ORIGINAL'
            }
            else {
                $status = 'DUPLICATE'
                $totalDupeFiles++
                $totalReclaimBytes += $file.Length
            }
            $isFirst = $false

            $row = New-Object PSObject -Property ([ordered]@{
                Hash      = $hash
                Status    = $status
                FilePath  = $file.FullName
                SizeBytes = $file.Length
                SizeHuman = Format-FileSize -Bytes $file.Length
                FileName  = $file.Name
            })
            $reportRows.Add($row)
        }
    }

    # Write the report CSV (overwrites any previous run)
    $reportRows | Export-Csv -Path $ReportCsvPath -NoTypeInformation -Encoding UTF8
    Write-Host "  Report written to: $ReportCsvPath" -ForegroundColor Green
    Write-Host ''

    # ------------------------------------------------------------------
    # CONSOLE SUMMARY
    # ------------------------------------------------------------------
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host '  SCAN SUMMARY' -ForegroundColor Cyan
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host ('  Media files scanned        : {0,8}' -f $allFiles.Count)    -ForegroundColor White
    Write-Host ('  Candidate files hashed     : {0,8}' -f $candidateCount)    -ForegroundColor White
    Write-Host ('  Duplicate groups           : {0,8}' -f $dupGroupCount)     -ForegroundColor White
    Write-Host ('  Duplicate files (deletable): {0,8}' -f $totalDupeFiles)    -ForegroundColor Yellow
    Write-Host ('  Space reclaimable          : {0}'   -f (Format-FileSize -Bytes $totalReclaimBytes)) `
               -ForegroundColor Green
    if ($enumErrors.Count -gt 0) {
        Write-Host ('  Errors / skipped           : {0,8} (see {1})' -f $enumErrors.Count, $ErrorLogPath) `
                   -ForegroundColor Red
    }
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host ''

    # ------------------------------------------------------------------
    # PRINT DUPLICATE GROUPS TO CONSOLE (capped to avoid flooding output)
    # ------------------------------------------------------------------
    $printed = 0
    foreach ($group in $dupGroups) {
        if ($printed -ge $MaxConsoleGroups) {
            $remaining = $dupGroupCount - $MaxConsoleGroups
            Write-Host "  ... $($remaining) more group(s) -- see $ReportCsvPath for the full list." `
                       -ForegroundColor DarkGray
            break
        }
        $printed++

        $files = $group.Value | Sort-Object { $_.FullName.Length }
        $shortHash = $group.Key.Substring(0, 16)
        Write-Host "  Hash : $($shortHash)..." -ForegroundColor Magenta
        Write-Host ('  Size : {0}' -f (Format-FileSize -Bytes $files[0].Length)) -ForegroundColor DarkGray

        $isFirst = $true
        foreach ($file in $files) {
            if ($isFirst) {
                Write-Host "    [KEEP]  $($file.FullName)" -ForegroundColor Green
            }
            else {
                Write-Host "    [DUPE]  $($file.FullName)" -ForegroundColor Red
            }
            $isFirst = $false
        }
        Write-Host ''
    }

    return $reportRows
}

# -----------------------------------------------------------------------------
# PHASE 2 -- DELETION
# -----------------------------------------------------------------------------

function Invoke-DuplicateDeletion {
    param(
        # Pass scan results directly to avoid re-reading the CSV
        [System.Collections.Generic.List[PSCustomObject]]$InMemoryReport,
        # Alternatively, read from a previously generated CSV
        [string]$CsvReportPath
    )

    # Detect whether -WhatIf was passed on the command line
    $isDryRun = ($WhatIfPreference -eq [System.Management.Automation.ActionPreference]::Continue)

    Write-Host ''
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host '  PHASE 2 -- DUPLICATE FILE DELETION' -ForegroundColor Cyan
    if ($isDryRun) {
        Write-Host '  *** DRY-RUN (-WhatIf) -- no files will be deleted ***' -ForegroundColor Yellow
    }
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host ''

    # ------------------------------------------------------------------
    # Load the report -- in-memory results take priority over a CSV file
    # ------------------------------------------------------------------
    $reportData = $null

    if ($null -ne $InMemoryReport -and $InMemoryReport.Count -gt 0) {
        Write-Host '  Using in-memory scan results.' -ForegroundColor DarkGray
        $reportData = $InMemoryReport
    }
    elseif ($CsvReportPath -and (Test-Path -LiteralPath $CsvReportPath)) {
        Write-Host "  Loading report from: $CsvReportPath" -ForegroundColor DarkGray
        $reportData = Import-Csv -Path $CsvReportPath -Encoding UTF8
    }
    else {
        Write-Host '  ERROR: No report data found. Run Phase 1 first or supply -ReportPath.' `
                   -ForegroundColor Red
        return
    }

    # Filter to rows marked DUPLICATE only
    $toDelete = @($reportData | Where-Object { $_.Status -eq 'DUPLICATE' })

    if ($toDelete.Count -eq 0) {
        Write-Host '  No files marked DUPLICATE in the report. Nothing to delete.' `
                   -ForegroundColor Green
        return
    }

    # Compute totals for the confirmation banner
    $totalBytes = [long]($toDelete | Measure-Object -Property SizeBytes -Sum).Sum
    $totalSizeStr = Format-FileSize -Bytes $totalBytes

    Write-Host "  Files marked for deletion : $($toDelete.Count)" -ForegroundColor Yellow
    Write-Host "  Total space to recover    : $($totalSizeStr)"   -ForegroundColor Yellow
    Write-Host ''

    # Show a preview of the first 10 candidates
    $previewMax = [Math]::Min(10, $toDelete.Count)
    Write-Host "  Preview (first $($previewMax)):" -ForegroundColor DarkGray
    for ($i = 0; $i -lt $previewMax; $i++) {
        Write-Host "    [-] $($toDelete[$i].FilePath)" -ForegroundColor Red
    }
    if ($toDelete.Count -gt $previewMax) {
        $extra = $toDelete.Count - $previewMax
        Write-Host "    ... and $($extra) more. See $ReportCsvPath." -ForegroundColor DarkGray
    }
    Write-Host ''

    # ------------------------------------------------------------------
    # In dry-run mode: show intent and exit without touching any files
    # ------------------------------------------------------------------
    if ($isDryRun) {
        Write-Host "  [WhatIf] Would delete $($toDelete.Count) file(s) and recover $($totalSizeStr)." `
                   -ForegroundColor Yellow
        Write-Host '  Re-run without -WhatIf to perform actual deletion.' -ForegroundColor DarkGray
        Write-Host ''
        return
    }

    # ------------------------------------------------------------------
    # Require explicit user confirmation before deleting anything
    # ------------------------------------------------------------------
    Write-Host '  +--------------------------------------------------+' -ForegroundColor Red
    Write-Host '  |  WARNING: About to PERMANENTLY delete files.      |' -ForegroundColor Red
    Write-Host "  |  Count : $($toDelete.Count) file(s)" -ForegroundColor Red
    Write-Host "  |  Size  : $($totalSizeStr)" -ForegroundColor Red
    Write-Host '  |  This action cannot be undone.                    |' -ForegroundColor Red
    Write-Host '  +--------------------------------------------------+' -ForegroundColor Red
    Write-Host ''

    $answer = Read-Host '  Proceed with deletion? [Y/N]'
    if ($answer -notmatch '^[Yy]$') {
        Write-Host '  Deletion cancelled.' -ForegroundColor Yellow
        return
    }
    Write-Host ''

    # ------------------------------------------------------------------
    # DELETE LOOP
    # ------------------------------------------------------------------
    $deletionLog  = New-Object 'System.Collections.Generic.List[PSCustomObject]'
    $deletedCount = 0
    $deletedBytes = [long]0
    $failedCount  = 0
    $skippedCount = 0
    $progress     = 0
    $deleteTotal  = $toDelete.Count

    foreach ($item in $toDelete) {
        $progress++
        $pct = [int](($progress / $deleteTotal) * 100)

        Write-Progress -Activity 'Deleting duplicate files' `
                       -Status "[$($progress) / $($deleteTotal)]  $([IO.Path]::GetFileName($item.FilePath))" `
                       -PercentComplete $pct `
                       -CurrentOperation $item.FilePath

        # Build a log entry regardless of outcome
        $logEntry = New-Object PSObject -Property ([ordered]@{
            Timestamp = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
            FilePath  = $item.FilePath
            SizeBytes = $item.SizeBytes
            SizeHuman = $item.SizeHuman
            Hash      = $item.Hash
            Result    = ''
            Error     = ''
        })

        # Guard 1 -- file must still exist on disk
        if (-not (Test-Path -LiteralPath $item.FilePath -PathType Leaf)) {
            $logEntry.Result = 'SKIPPED_NOT_FOUND'
            $logEntry.Error  = 'File no longer exists on disk'
            $deletionLog.Add($logEntry)
            $skippedCount++
            Write-Log -Message "Not found (skipped): $($item.FilePath)" -Level WARN
            continue
        }

        # Guard 2 -- safety check: ensure there IS a corresponding ORIGINAL for this hash.
        #            If the report is malformed and no ORIGINAL exists, skip to avoid
        #            destroying the only remaining copy of a file.
        $hasOriginal = $reportData | Where-Object { $_.Hash -eq $item.Hash -and $_.Status -eq 'ORIGINAL' }
        if (-not $hasOriginal) {
            $logEntry.Result = 'SKIPPED_NO_ORIGINAL'
            $logEntry.Error  = 'No ORIGINAL row found for this hash -- skipped for safety'
            $deletionLog.Add($logEntry)
            $skippedCount++
            Write-Log -Message "No ORIGINAL for hash $($item.Hash) -- skipped: $($item.FilePath)" -Level WARN
            continue
        }

        # Perform the deletion.
        # ShouldProcess returns $true normally and respects -Confirm if passed.
        # WhatIf is already handled above so this branch is never reached in dry-run.
        if ($PSCmdlet.ShouldProcess($item.FilePath, 'Remove duplicate file')) {
            try {
                Remove-Item -LiteralPath $item.FilePath -Force -ErrorAction Stop
                $logEntry.Result  = 'DELETED'
                $deletedCount++
                $deletedBytes += [long]$item.SizeBytes
            }
            catch {
                $logEntry.Result = 'FAILED'
                $logEntry.Error  = $_.Exception.Message
                $failedCount++
                Write-Log -Message "Delete failed: $($item.FilePath) -- $($_.Exception.Message)" -Level ERROR
            }
        }

        $deletionLog.Add($logEntry)
    }

    Write-Progress -Activity 'Deleting duplicate files' -Completed

    # Append this run's results to the deletion log (creates file if it does not exist)
    $deletionLog | Export-Csv -Path $DeletionLogPath -NoTypeInformation -Encoding UTF8 -Append

    # ------------------------------------------------------------------
    # DELETION SUMMARY
    # ------------------------------------------------------------------
    Write-Host ''
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host '  DELETION SUMMARY' -ForegroundColor Cyan
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host ('  Successfully deleted : {0,6} file(s)' -f $deletedCount) -ForegroundColor Green
    Write-Host ('  Space recovered      : {0}'           -f (Format-FileSize -Bytes $deletedBytes)) `
               -ForegroundColor Green
    if ($skippedCount -gt 0) {
        Write-Host ('  Skipped (not found)  : {0,6} file(s)' -f $skippedCount) -ForegroundColor Yellow
    }
    if ($failedCount -gt 0) {
        Write-Host ('  Failed to delete     : {0,6} file(s) (see {1})' -f $failedCount, $ErrorLogPath) `
                   -ForegroundColor Red
    }
    Write-Host "  Deletion log         : $DeletionLogPath" -ForegroundColor White
    Write-Host ('=' * 57) -ForegroundColor Cyan
    Write-Host ''
}

# -----------------------------------------------------------------------------
# MAIN -- wire Phase 1 and Phase 2 together based on parameters
# -----------------------------------------------------------------------------

# Write a run-start marker to the error log so successive runs are easy to tell apart
$runSep = '-' * 60
Add-Content -Path $ErrorLogPath `
    -Value "$runSep`n[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] RUN STARTED`n$runSep" `
    -Encoding UTF8

if ($Delete -and $ReportPath -and (Test-Path -LiteralPath $ReportPath)) {
    # Deletion only: skip the scan, use an existing CSV report
    Invoke-DuplicateDeletion -CsvReportPath $ReportPath

}
elseif ($Delete) {
    # Full run: scan first, then delete from in-memory results
    $scanResults = Invoke-DuplicateScan
    Invoke-DuplicateDeletion -InMemoryReport $scanResults

}
else {
    # Report only (default): scan and write the CSV, no deletion
    Invoke-DuplicateScan | Out-Null
    Write-Host '  Review the report at:' -ForegroundColor Cyan
    Write-Host "    $ReportCsvPath" -ForegroundColor White
    Write-Host ''
    Write-Host '  Then re-run with one of:' -ForegroundColor DarkGray
    Write-Host '    -Delete                              scan again + delete' -ForegroundColor DarkGray
    Write-Host '    -Delete -WhatIf                      dry-run (no files touched)' -ForegroundColor DarkGray
    Write-Host "    -Delete -ReportPath `"$ReportCsvPath`"  delete from existing report" `
               -ForegroundColor DarkGray
    Write-Host ''
}
