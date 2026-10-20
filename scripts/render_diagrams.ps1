# docs/diagrams/*.html을 docs/images/*.png로 렌더링합니다.
# 설치된 Edge(없으면 Chrome)의 headless 스크린샷 기능을 사용하므로 추가 패키지가 필요 없습니다.
# 사용법: .\scripts\render_diagrams.ps1            (전체)
#         .\scripts\render_diagrams.ps1 -Name 01_overview   (한 장만)
param(
    [string]$Name = "*",   # 렌더링할 HTML 파일 이름(확장자 제외). 기본값은 전체
    [int]$Scale = 2         # 픽셀 배율. 2이면 1600x900 화면을 3200x1800 PNG로 저장해 README에서도 선명합니다
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$sourceDir = Join-Path $root "docs\diagrams"
$outputDir = Join-Path $root "docs\images"
New-Item -ItemType Directory -Force $outputDir | Out-Null

# 브라우저 경로 후보. 먼저 찾은 것을 사용합니다.
$browser = @(
    "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe",
    "$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe",
    "$env:ProgramFiles\Google\Chrome\Application\chrome.exe"
) | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $browser) { throw "Edge 또는 Chrome을 찾지 못했습니다." }

# 실행 중인 일반 브라우저 창과 섞이지 않도록 임시 프로필 폴더를 따로 씁니다.
$profileDir = Join-Path $env:TEMP "sports-prediction-diagram-profile"

Get-ChildItem $sourceDir -Filter "$Name.html" | ForEach-Object {
    $target = Join-Path $outputDir ($_.BaseName + ".png")
    $url = "file:///" + ($_.FullName -replace "\\", "/")
    # 브라우저가 stderr에 남기는 무해한 경고 때문에 스크립트가 멈추지 않도록 Start-Process로 실행합니다.
    $arguments = @("--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
        "--user-data-dir=`"$profileDir`"", "--force-device-scale-factor=$Scale",
        "--window-size=1600,900", "--screenshot=`"$target`"", "`"$url`"")
    Start-Process -FilePath $browser -ArgumentList $arguments -Wait -WindowStyle Hidden
    if (-not (Test-Path $target)) { throw "렌더링 실패: $($_.Name)" }
    Write-Output "생성: docs/images/$($_.BaseName).png"
}
