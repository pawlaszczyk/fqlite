
$ErrorActionPreference = 'Stop'
$toolsDir   = "$(Split-Path -parent $MyInvocation.MyCommand.Definition)"
$url64      = "https://github.com/pawlaszczyk/fqlite/releases/download/{{VERSION}}/fqlite-{{VERSION}}-windows.exe"

$packageArgs = @{
  packageName   = $env:ChocolateyPackageName
  unzipLocation = $toolsDir
  fileType      = 'exe'
  url64bit      = $url64

  softwareName  = 'fqlite*'

  checksum64    = "{{HASH}}"
  checksumType64= 'sha256'

  validExitCodes= @(0, 3010, 1641)
  silentArgs   =  '/quiet'
}

Install-ChocolateyPackage @packageArgs
