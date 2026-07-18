<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;

/**
 * @implements Encoder<array<string, mixed>>
 */
final class PostgreSqlEncoder implements Encoder
{
    public function decode(array $batch): array
    {
        $decoded = [];

        foreach ($batch as $values) {
            $decoded[] = new RawRowValues($values);
        }

        return $decoded;
    }

    public function encode(array $batch): array
    {
        $encoded = [];

        foreach ($batch as $rowValues) {
            $encoded[] = $rowValues->values;
        }

        return $encoded;
    }
}
