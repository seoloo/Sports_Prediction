<#
Claude Code 세션을 데스크톱 앱과 분리된 백그라운드로 띄우고 원격 제어(Remote Control)를 켠다.

왜: 데스크톱 앱은 쉬는 동안 스스로 업데이트하며 재시작하고, 그때 앱 안의 세션과 원격 제어 연결이 끊긴다.
`claude --bg`로 띄운 세션은 Claude Code CLI의 백그라운드 데몬이 맡으므로 앱 재시작과 무관하게 계속 돈다.

사용:
  .\scripts\start_remote_session.ps1            # 이 저장소 세션이 없으면 새로 띄움(있으면 그대로 둠)
  .\scripts\start_remote_session.ps1 -Status    # 이 저장소의 백그라운드 세션 상태만 보기

띄운 뒤:
  휴대폰·웹: claude.ai/code 에서 "Sports_Prediction" 세션
  PowerShell: claude attach Sports_Prediction   (빠져나오기는 화면 안내를 따름, 세션은 계속 돎)
  중지: claude stop <id>   /  새 버전으로 다시 시작: claude respawn <id>
PC를 재부팅하면 백그라운드 세션도 꺼지므로 이 스크립트를 다시 실행한다.
#>
param(
    [string]$Name = "Sports_Prediction",
    [switch]$Status
)
$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$claude = (Get-Command claude -ErrorAction SilentlyContinue).Source
if (-not $claude) { $claude = Join-Path $env:USERPROFILE ".local\bin\claude.exe" }
if (-not (Test-Path $claude)) { throw "claude CLI를 찾을 수 없습니다. https://claude.com/claude-code 안내대로 설치하세요." }

# 이 저장소에서 이미 돌고 있는 백그라운드 세션(같은 이름)이 있으면 새로 만들지 않는다.
$running = @(& $claude agents --json | ConvertFrom-Json | Where-Object {
    $_.kind -eq "background" -and $_.cwd -eq $repo -and $_.name -eq $Name })
if ($Status -or $running.Count -gt 0) {
    if ($running.Count -eq 0) { Write-Output "이 저장소의 백그라운드 세션이 없습니다." }
    $running | Select-Object id, name, status, state, pid | Format-Table -AutoSize
    exit 0
}

Push-Location $repo
try {
    & $claude --bg --remote-control $Name --name $Name
} finally {
    Pop-Location
}
