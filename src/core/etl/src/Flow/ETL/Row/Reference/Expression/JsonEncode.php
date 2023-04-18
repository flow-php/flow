<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class JsonEncode implements Expression
{
    public function __construct(private readonly Expression $ref, private readonly int $flags = JSON_THROW_ON_ERROR)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        try {
            return \json_encode($value, $this->flags);
            /** @phpstan-ignore-next-line  */
        } catch (\JsonException $e) {
            return null;
        }
    }
}
