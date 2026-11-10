-- Inicializa la BD hospital con CDC y datos de ejemplo.
-- Es idempotente: se puede ejecutar varias veces sin duplicar nada.

IF DB_ID('hospital') IS NULL
    CREATE DATABASE hospital;
GO

USE hospital;
GO

-- CDC a nivel de base de datos
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'hospital') = 0
    EXEC sys.sp_cdc_enable_db;
GO

IF OBJECT_ID('dbo.pacientes') IS NULL
    CREATE TABLE dbo.pacientes (
        paciente_id     INT PRIMARY KEY,
        nombre          NVARCHAR(100) NOT NULL,
        edad            INT NOT NULL,
        genero          CHAR(1) CHECK (genero IN ('M','F')),
        email           NVARCHAR(100) NOT NULL,
        historia_medica NVARCHAR(255)
    );
GO

-- CDC sobre la tabla
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables ct
    JOIN sys.tables t ON ct.source_object_id = t.object_id
    WHERE t.name = 'pacientes'
)
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'pacientes',
        @role_name     = NULL;
GO

IF NOT EXISTS (SELECT 1 FROM dbo.pacientes)
    INSERT INTO dbo.pacientes (paciente_id, nombre, edad, genero, email, historia_medica) VALUES
        (1, N'Juan Pérez',     45, 'M', N'juan.perez@example.com',     N'Hipertensión'),
        (2, N'María López',    52, 'F', N'maria.lopez@example.com',    N'Diabetes tipo 2'),
        (3, N'Carlos Ruiz',    38, 'M', N'carlos.ruiz@example.com',    N'Sin antecedentes'),
        (4, N'Ana Torres',     60, 'F', N'ana.torres@example.com',     N'Arritmias'),
        (5, N'Pedro Gómez',    70, 'M', N'pedro.gomez@example.com',    N'Cardiopatía'),
        (6, N'Laura Sánchez',  29, 'F', N'laura.sanchez@example.com',  N'Sin antecedentes'),
        (7, N'Miguel Ortega',  50, 'M', N'miguel.ortega@example.com',  N'Hipertensión y obesidad');
GO

-- Comprobación
SELECT COUNT(*) AS filas FROM dbo.pacientes;
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = 'pacientes';
GO
