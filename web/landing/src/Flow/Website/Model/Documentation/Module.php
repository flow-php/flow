<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

use function Flow\Types\DSL\type_instance_of;
use function mb_strtoupper;
use function str_replace;

enum Module: string
{
    case AVRO = 'Avro';
    case AZURE_FILESYSTEM = 'Azure Filesystem';
    case AZURE_SDK = 'Azure SDK';
    case CHART_JS = 'Chart.js';
    case CORE = 'Core';
    case CSV = 'CSV';
    case DOCTRINE = 'Doctrine';
    case EXCEL = 'Excel';
    case FILESYSTEM = 'Filesystem';
    case FLOE = 'Floe';
    case GOOGLE_SHEET = 'Google Sheet';
    case HTTP = 'HTTP';
    case JSON = 'JSON';
    case MEILI_SEARCH = 'Meili Search';
    case MONOLOG_TELEMETRY_BRIDGE = 'Monolog Telemetry Bridge';
    case PARQUET = 'Parquet';
    case PG_QUERY = 'PG_QUERY';
    case POSTGRESQL = 'POSTGRESQL';
    case POSTGRESQL_MIGRATIONS = 'PostgreSQL Migrations';
    case POSTGRESQL_VALINOR_BRIDGE = 'PostgreSQL Valinor Bridge';
    case PSR18_TELEMETRY_BRIDGE = 'PSR-18 Telemetry Bridge';
    case PSR3_TELEMETRY_BRIDGE = 'PSR-3 Telemetry Bridge';
    case PSR7_TELEMETRY_BRIDGE = 'PSR-7 Telemetry Bridge';
    case S3_FILESYSTEM = 'S3 Filesystem';
    case SEAL = 'Seal';
    case SFTP_FILESYSTEM = 'SFTP Filesystem';
    case SYMFONY_HTTP_FOUNDATION_TELEMETRY_BRIDGE = 'Symfony HttpFoundation Telemetry Bridge';
    case SYMFONY_TELEMETRY_BUNDLE = 'Symfony Telemetry Bundle';
    case TELEMETRY = 'Telemetry';
    case TELEMETRY_OTLP = 'Telemetry OTLP';
    case TEXT = 'Text';
    case TYPES = 'Types';
    case XML = 'XML';

    public static function fromName(string $name): self
    {
        $name = mb_strtoupper(str_replace([' ', '-'], '_', $name));

        return type_instance_of(self::class)->assert(constant("self::{$name}"));
    }

    public function priority(): int
    {
        return match ($this) {
            self::CORE => 1,
            self::CSV => 2,
            self::DOCTRINE => 3,
            self::POSTGRESQL => 4,
            self::FLOE => 5,
            self::GOOGLE_SHEET => 6,
            self::CHART_JS => 7,
            self::JSON => 8,
            self::MEILI_SEARCH => 9,
            self::PARQUET => 10,
            self::TEXT => 11,
            self::XML => 12,
            self::FILESYSTEM => 13,
            self::TYPES => 14,
            self::AZURE_FILESYSTEM => 15,
            self::AZURE_SDK => 16,
            self::HTTP => 17,
            self::EXCEL => 18,
            self::PG_QUERY => 19,
            self::TELEMETRY => 20,
            self::TELEMETRY_OTLP => 21,
            self::MONOLOG_TELEMETRY_BRIDGE => 22,
            self::SYMFONY_HTTP_FOUNDATION_TELEMETRY_BRIDGE => 23,
            self::SYMFONY_TELEMETRY_BUNDLE => 24,
            self::PSR7_TELEMETRY_BRIDGE => 25,
            self::PSR18_TELEMETRY_BRIDGE => 26,
            self::POSTGRESQL_MIGRATIONS => 27,
            self::POSTGRESQL_VALINOR_BRIDGE => 28,
            self::PSR3_TELEMETRY_BRIDGE => 29,
            self::SEAL => 30,
            self::SFTP_FILESYSTEM => 31,
            default => 99,
        };
    }
}
