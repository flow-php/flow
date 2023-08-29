<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class JsonDecode implements Expression
{
    public function __construct(private readonly Expression $ref, private readonly int $flags = JSON_THROW_ON_ERROR)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (!\is_string($value)) {
            return null;
        }

        try {
            return \json_decode($value, true, 512, $this->flags);
        } catch (\JsonException $e) {
            return null;
        }
    }
}
