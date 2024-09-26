<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Output;

enum Type
{
    case CSV;
    case JSON;
    case PARQUET;
    case TEXT;
    case XML;

    public function toContentTypeHeader() : string
    {
        return match ($this) {
            self::JSON => 'application/json',
            self::CSV => 'text/csv',
            self::XML => 'application/xml',
            self::PARQUET => 'application/parquet',
            self::TEXT => 'text/plain',
        };
    }
}
