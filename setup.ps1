# ============================================
# Setup del Data Pipeline Framework
# ============================================
# Este script automatiza toda la instalacion del framework
# NO requiere ejecutar scripts SQL manualmente
# ============================================

Write-Host "`n================================================" -ForegroundColor Cyan
Write-Host "   DATA PIPELINE FRAMEWORK - SETUP" -ForegroundColor Cyan
Write-Host "================================================`n" -ForegroundColor Cyan

Write-Host "[1/6] Verificando dependencias..." -ForegroundColor Yellow

# Verificar Python
$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Python: $pythonVersion" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Python no instalado" -ForegroundColor Red
    exit 1
}

# Verificar Docker
$dockerVersion = docker --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Docker: $dockerVersion" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Docker no instalado" -ForegroundColor Red
    exit 1
}

# Verificar que Docker este corriendo
$dockerRunning = docker ps 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Docker corriendo" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Docker no esta corriendo. Iniciar Docker Desktop" -ForegroundColor Red
    exit 1
}

# Crear entorno virtual si no existe
if (-Not (Test-Path ".venv")) {
    Write-Host "  [*] Creando entorno virtual..." -ForegroundColor Cyan
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [ERROR] Fallo al crear entorno virtual" -ForegroundColor Red
        exit 1
    }
    Write-Host "  [OK] Entorno virtual creado" -ForegroundColor Green
} else {
    Write-Host "  [OK] Entorno virtual existe" -ForegroundColor Green
}

& .venv\Scripts\Activate.ps1

Write-Host "`n[2/6] Instalando dependencias Python..." -ForegroundColor Yellow

# Actualizar pip primero para mejorar manejo de rutas
.venv\Scripts\python.exe -m pip install --upgrade pip | Out-Null

.venv\Scripts\pip.exe install -q -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [ERROR] Fallo la instalacion de paquetes." -ForegroundColor Red
    Write-Host "  "
    Write-Host "  [!] POSIBLE SOLUCION: LIMITACION DE RUTAS LARGAS DE WINDOWS" -ForegroundColor Yellow
    Write-Host "  Parece un error de 'Long Path' (ruta demasiado larga)."
    Write-Host "  Ejecuta este comando en PowerShell como Administrador para solucionarlo:"
    Write-Host "  "
    Write-Host "      Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' -Name 'LongPathsEnabled' -Value 1"
    Write-Host "  "
    Write-Host "  Luego reinicia la terminal y vuelve a ejecutar setup.ps1"
    exit 1
}
Write-Host "  [OK] Dependencias instaladas" -ForegroundColor Green

Write-Host "`n[3/6] Iniciando PostgreSQL..." -ForegroundColor Yellow

docker stop framework_postgres 2>$null | Out-Null
docker rm framework_postgres 2>$null | Out-Null

docker-compose up -d postgres | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [ERROR] Fallo al iniciar PostgreSQL" -ForegroundColor Red
    exit 1
}

Write-Host "  [*] Esperando PostgreSQL..." -ForegroundColor Cyan
Start-Sleep -Seconds 8

# Verificar que PostgreSQL este corriendo
$retries = 0
$maxRetries = 10
while ($retries -lt $maxRetries) {
    $pgReady = docker exec framework_postgres pg_isready -U admin -d data_framework 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] PostgreSQL listo" -ForegroundColor Green
        break
    }
    $retries++
    Start-Sleep -Seconds 2
}

if ($retries -eq $maxRetries) {
    Write-Host "  [ERROR] PostgreSQL no responde" -ForegroundColor Red
    exit 1
}

Write-Host "`n[4/6] Inicializando base de datos..." -ForegroundColor Yellow

Get-Content scripts/init_db.sql | docker exec -i framework_postgres psql -U admin -d data_framework 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [ERROR] Fallo inicializacion DB" -ForegroundColor Red
    exit 1
}

Write-Host "  [OK] Base de datos inicializada" -ForegroundColor Green

Write-Host "`n[5/6] Generar datos de ejemplo (customers + transactions)? (y/n): " -ForegroundColor Yellow -NoNewline
$generateData = Read-Host

if ($generateData -eq "y" -or $generateData -eq "yes" -or $generateData -eq "s" -or $generateData -eq "si") {
    Write-Host "  [*] Generando 10,000 customers + 50,000 transactions..." -ForegroundColor Cyan
    .venv\Scripts\python.exe scripts/generate_sample_data.py -c 10000 -t 50000
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] Datos generados" -ForegroundColor Green
    }
    else {
        Write-Host "  [WARNING] Fallo la generacion" -ForegroundColor Yellow
    }
}
else {
    Write-Host "  [*] Omitido" -ForegroundColor Yellow
}

Write-Host "`n[6/7] Configurando comando global..." -ForegroundColor Yellow

# Crear funcion para comando global data-framework
$projectPath = $PWD.Path
$functionDefinition = @"

# Data Pipeline Framework - Global Command
function data-framework {
    `$originalPath = `$PWD
    Set-Location '$projectPath'
    
    if (-Not (Test-Path '.venv\Scripts\python.exe')) {
        Write-Host 'ERROR: Entorno virtual no encontrado. Ejecuta setup.ps1' -ForegroundColor Red
        Set-Location `$originalPath
        return 1
    }
    
    & .venv\Scripts\python.exe -m src.cli `$args
    `$exitCode = `$LASTEXITCODE
    Set-Location `$originalPath
    return `$exitCode
}
"@

# Crear perfil si no existe
if (-Not (Test-Path $PROFILE)) {
    New-Item -Path $PROFILE -ItemType File -Force | Out-Null
    Write-Host "  [*] Perfil creado: $PROFILE" -ForegroundColor Cyan
}

# Leer contenido actual del perfil
$profileContent = Get-Content $PROFILE -Raw -ErrorAction SilentlyContinue

# Verificar si ya existe la funcion
if ($profileContent -match "function data-framework") {
    # Reemplazar funcion existente
    $profileContent = $profileContent -replace "(?ms)# Data Pipeline Framework - Global Command.*?^}", $functionDefinition.TrimStart()
    Set-Content -Path $PROFILE -Value $profileContent
    Write-Host "  [OK] Comando actualizado" -ForegroundColor Green
} else {
    # Agregar nueva funcion
    Add-Content -Path $PROFILE -Value $functionDefinition
    Write-Host "  [OK] Comando 'data-framework' agregado" -ForegroundColor Green
}

Write-Host "  [*] Recarga el perfil: . `$PROFILE" -ForegroundColor Cyan

Write-Host "`n[7/7] Finalizando..." -ForegroundColor Yellow
Write-Host "  [OK] Setup completado" -ForegroundColor Green

Write-Host "`n================================================" -ForegroundColor Green
Write-Host "   SETUP COMPLETADO" -ForegroundColor Green
Write-Host "================================================`n" -ForegroundColor Green

Write-Host "USO DEL COMANDO GLOBAL:`n" -ForegroundColor Cyan
Write-Host "  1. Recargar perfil:    . `$PROFILE" -ForegroundColor White
Write-Host "  2. Ejecutar pipeline:  data-framework run pipeline -c examples/pipeline.yml" -ForegroundColor White
Write-Host "  3. Infectar datos:     data-framework infect -c examples/infection_config.yml" -ForegroundColor White
Write-Host "`nO USA EL METODO TRADICIONAL:`n" -ForegroundColor Cyan
Write-Host "  1. Activar entorno:    .venv\Scripts\Activate.ps1" -ForegroundColor White
Write-Host "  2. Ver estado DB:      python scripts/utils_db.py status" -ForegroundColor White
Write-Host "  3. Ejecutar pipeline:  data-framework run pipeline -c examples/pipeline.yml" -ForegroundColor White
Write-Host "`nCOMANDOS UTILES:`n" -ForegroundColor Cyan
Write-Host "  python scripts/utils_db.py status|stats|executions" -ForegroundColor White
Write-Host "  docker-compose down" -ForegroundColor Gray

Write-Host "================================================`n" -ForegroundColor Green
