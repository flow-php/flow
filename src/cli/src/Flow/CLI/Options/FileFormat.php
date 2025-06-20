<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

enum FileFormat : string
{
    case CSV = 'csv';
    case JSON = 'json';
    case ODS = 'ods';
    case PARQUET = 'parquet';
    case TEXT = 'txt';
    case XLSX = 'xlsx';
    case XML = 'xml';

    public static function isValid(string $format) : bool
    {
        return \in_array($format, \array_column(self::cases(), 'value'), true);
    }
}
