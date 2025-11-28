<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

enum Module : string
{
    case AVRO = 'Avro';
    case AZURE_FILESYSTEM = 'Azure Filesystem';
    case AZURE_SDK = 'Azure SDK';
    case CHART_JS = 'Chart.js';
    case CORE = 'Core';
    case CSV = 'CSV';
    case DOCTRINE = 'Doctrine';
    case ELASTIC_SEARCH = 'Elastic Search';
    case EXCEL = 'Excel';
    case FILESYSTEM = 'Filesystem';
    case GOOGLE_SHEET = 'Google Sheet';
    case HTTP = 'HTTP';
    case JSON = 'JSON';
    case MEILI_SEARCH = 'Meili Search';
    case PARQUET = 'Parquet';
    case PG_QUERY = 'PG_QUERY';
    case S3_FILESYSTEM = 'S3 Filesystem';
    case TEXT = 'Text';
    case TYPES = 'Types';
    case XML = 'XML';

    public static function fromName(string $name) : self
    {
        $name = \mb_strtoupper(\str_replace([' ', '-'], '_', $name));

        return constant("self::{$name}");
    }

    public function priority() : int
    {
        return match ($this) {
            self::CORE => 1,
            self::CSV => 2,
            self::DOCTRINE => 3,
            self::ELASTIC_SEARCH => 4,
            self::GOOGLE_SHEET => 5,
            self::CHART_JS => 6,
            self::JSON => 7,
            self::MEILI_SEARCH => 8,
            self::PARQUET => 9,
            self::TEXT => 10,
            self::XML => 11,
            self::FILESYSTEM => 12,
            self::TYPES => 13,
            self::AZURE_FILESYSTEM => 14,
            self::AZURE_SDK => 15,
            self::HTTP => 16,
            self::EXCEL => 17,
            self::PG_QUERY => 18,
            default => 99,
        };
    }
}
