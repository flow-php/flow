<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

enum FileFormat : string
{
    case CSV = 'csv';
    case JSON = 'json';
    case PARQUET = 'parquet';
    case TEXT = 'txt';
    case XML = 'xml';

    public function isValid(string $format) : bool
    {
        return \in_array($format, self::toArray(), true);
    }

    private static function toArray() : array
    {
        return [
            self::CSV,
            self::JSON,
            self::XML,
            self::PARQUET,
            self::TEXT,
        ];
    }
}
