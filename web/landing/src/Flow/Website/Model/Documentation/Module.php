<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

enum Module : string
{
    case AZURE_FILESYSTEM = 'Azure Filesystem';
    case AZURE_SDK = 'Azure SDK';
    case CHART_JS = 'Chart.js';
    case CORE = 'Core';
    case CSV = 'CSV';
    case DOCTRINE = 'Doctrine';
    case ELASTIC_SEARCH = 'Elastic Search';
    case FILESYSTEM = 'Filesystem';
    case GOOGLE_SHEET = 'Google Sheet';
    case JSON = 'JSON';
    case MEILI_SEARCH = 'Meili Search';
    case PARQUET = 'Parquet';
    case S3_FILESYSTEM = 'S3 Filesystem';
    case TEXT = 'Text';
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
            self::AZURE_FILESYSTEM => 13,
            self::AZURE_SDK => 14,
            default => 99,
        };
    }
}
