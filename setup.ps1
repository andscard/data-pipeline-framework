# ============================================
# Setup del Data Pipeline Framework
# ============================================
# Este script automatiza toda la instalacion del framework
# NO requiere ejecutar scripts SQL manualmente
# ============================================

Write-Host "`n================================================" -ForegroundColor Cyan
Write-Host "   DATA PIPELINE FRAMEWORK - SETUP AUTOMATICO" -ForegroundColor Cyan
Write-Host "================================================`n" -ForegroundColor Cyan

# ============================================
# PASO 1: Verificar dependencias y crear entorno virtual
# ============================================

Write-Host "[1/6] Verificando dependencias...`n" -ForegroundColor Yellow

# Verificar Python
$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Python instalado: $pythonVersion" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Python no esta instalado" -ForegroundColor Red
    Write-Host "     Instalar desde: https://www.python.org/downloads/" -ForegroundColor Red
    exit 1
}

# Verificar Docker
$dockerVersion = docker --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Docker instalado: $dockerVersion" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Docker no esta instalado" -ForegroundColor Red
    Write-Host "     Instalar Docker Desktop desde: https://www.docker.com/products/docker-desktop" -ForegroundColor Red
    exit 1
}

# Verificar que Docker este corriendo
$dockerRunning = docker ps 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Docker esta corriendo" -ForegroundColor Green
} else {
    Write-Host "  [ERROR] Docker no esta corriendo" -ForegroundColor Red
    Write-Host "     Iniciar Docker Desktop" -ForegroundColor Red
    exit 1
}

# Crear entorno virtual si no existe
if (-Not (Test-Path ".venv")) {
    Write-Host "`n  [*] Creando entorno virtual (.venv)..." -ForegroundColor Cyan
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [ERROR] No se pudo crear el entorno virtual" -ForegroundColor Red
        exit 1
    }
    Write-Host "  [OK] Entorno virtual creado" -ForegroundColor Green
} else {
    Write-Host "`n  [OK] Entorno virtual (.venv) ya existe" -ForegroundColor Green
}

# Activar entorno virtual
Write-Host "  [*] Activando entorno virtual..." -ForegroundColor Cyan
& .venv\Scripts\Activate.ps1

# ============================================
# PASO 2: Instalar dependencias de Python
# ============================================

Write-Host "`n[2/6] Instalando dependencias de Python en .venv...`n" -ForegroundColor Yellow

.venv\Scripts\pip.exe install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [ERROR] Fallo la instalacion de dependencias" -ForegroundColor Red
    exit 1
}
Write-Host "  [OK] Dependencias instaladas correctamente" -ForegroundColor Green

# ============================================
# PASO 3: Iniciar PostgreSQL con Docker
# ============================================

Write-Host "`n[3/6] Iniciando PostgreSQL...`n" -ForegroundColor Yellow

# Detener contenedor existente si existe
docker stop framework_postgres 2>$null
docker rm framework_postgres 2>$null

# Iniciar servicios
docker-compose up -d postgres
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [ERROR] Fallo al iniciar PostgreSQL" -ForegroundColor Red
    exit 1
}

Write-Host "  [*] Esperando a que PostgreSQL este listo..." -ForegroundColor Yellow
Start-Sleep -Seconds 8

# Verificar que PostgreSQL este corriendo
$retries = 0
$maxRetries = 10
while ($retries -lt $maxRetries) {
    $pgReady = docker exec framework_postgres pg_isready -U admin -d data_framework 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] PostgreSQL esta listo" -ForegroundColor Green
        break
    }
    $retries++
    Write-Host "  [*] Reintento $retries/$maxRetries..." -ForegroundColor Yellow
    Start-Sleep -Seconds 2
}

if ($retries -eq $maxRetries) {
    Write-Host "  [ERROR] PostgreSQL no responde" -ForegroundColor Red
    exit 1
}

# ============================================
# PASO 4: Inicializar base de datos
# ============================================

Write-Host "`n[4/6] Inicializando base de datos...`n" -ForegroundColor Yellow

Get-Content scripts/init_db.sql | docker exec -i framework_postgres psql -U admin -d data_framework
if ($LASTEXITCODE -ne 0) {
    Write-Host "  [ERROR] Fallo la inicializacion de la base de datos" -ForegroundColor Red
    exit 1
}

Write-Host "  [OK] Base de datos inicializada correctamente" -ForegroundColor Green

# Verificar tablas creadas
Write-Host "`n  [*] Verificando estructura de base de datos...`n" -ForegroundColor Cyan
.venv\Scripts\python.exe scripts/db_utils.py status

# ============================================
# PASO 5: Generar datos de ejemplo
# ============================================

Write-Host "`n[5/6] Deseas generar datos de ejemplo? (y/n): " -ForegroundColor Yellow -NoNewline
$generateData = Read-Host

if ($generateData -eq "y" -or $generateData -eq "yes" -or $generateData -eq "s" -or $generateData -eq "si") {
    Write-Host ""
    Write-Host "  [*] Generando datos sinteticos..." -ForegroundColor Cyan
    Write-Host ""
    .venv\Scripts\python.exe scripts/generate_sample_data.py -c 10000 -t 50000
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "  [OK] Datos de ejemplo generados" -ForegroundColor Green
    }
    else {
        Write-Host ""
        Write-Host "  [WARNING] Fallo la generacion de datos" -ForegroundColor Yellow
        Write-Host "     Puedes generarlos mas tarde con: .venv\Scripts\python.exe scripts/generate_sample_data.py" -ForegroundColor Yellow
    }
}
else {
    Write-Host ""
    Write-Host "  [*] Omitiendo generacion de datos" -ForegroundColor Yellow
    Write-Host "     Puedes generarlos mas tarde con: .venv\Scripts\python.exe scripts/generate_sample_data.py" -ForegroundColor Yellow
}

# ============================================
# PASO 6: Instrucciones finales
# ============================================

Write-Host "`n[6/6] Configuracion final...`n" -ForegroundColor Yellow
Write-Host "  [OK] Entorno virtual configurado en: .venv\" -ForegroundColor Green
Write-Host "  [OK] Para activar el entorno: .venv\Scripts\Activate.ps1" -ForegroundColor Green

# ============================================
# FINALIZACION
# ============================================

Write-Host "`n================================================" -ForegroundColor Green
Write-Host "   [OK] SETUP COMPLETADO EXITOSAMENTE" -ForegroundColor Green
Write-Host "================================================`n" -ForegroundColor Green

Write-Host "SIGUIENTES PASOS:`n" -ForegroundColor Cyan
Write-Host "  1. Activar entorno virtual:" -ForegroundColor White
Write-Host "     .venv\Scripts\Activate.ps1`n" -ForegroundColor Gray

Write-Host "  2. Ver estado de la base de datos:" -ForegroundColor White
Write-Host "     python scripts/db_utils.py status`n" -ForegroundColor Gray

Write-Host "  3. Ejecutar un pipeline de ejemplo:" -ForegroundColor White
Write-Host "     python -m src.cli run pipeline -c examples/simple_pipeline.yml" -ForegroundColor Gray
Write-Host "     (o usar complete_pipeline.yml para sintaxis completa)`n" -ForegroundColor DarkGray

Write-Host "  4. Ver el reporte HTML generado en:" -ForegroundColor White
Write-Host "     reports/`n" -ForegroundColor Gray

Write-Host "DOCUMENTACION:`n" -ForegroundColor Cyan
Write-Host "  - README.md                : Guia completa del framework" -ForegroundColor White
Write-Host "  - docs/DATABASE.md         : Gestion de base de datos (db_utils.py)" -ForegroundColor White
Write-Host "  - docs/VALIDATION_TYPES.md : Tipos de validacion simplificada" -ForegroundColor White
Write-Host "  - examples/                : Ejemplos de configuracion de pipelines`n" -ForegroundColor White

Write-Host "UTILIDADES DB (python scripts/db_utils.py):`n" -ForegroundColor Cyan
Write-Host "  - Ver estado:         python scripts/db_utils.py status" -ForegroundColor White
Write-Host "  - Ver estadisticas:   python scripts/db_utils.py stats" -ForegroundColor White
Write-Host "  - Ver ejecuciones:    python scripts/db_utils.py executions" -ForegroundColor White
Write-Host "  - Limpiar datos:      python scripts/db_utils.py clean-samples" -ForegroundColor White
Write-Host "  - Ver ayuda completa: python scripts/db_utils.py --help`n" -ForegroundColor Gray

Write-Host "OTROS COMANDOS:`n" -ForegroundColor Cyan
Write-Host "  - Detener servicios:" -ForegroundColor White
Write-Host "    docker-compose down`n" -ForegroundColor Gray

Write-Host "================================================`n" -ForegroundColor Green
