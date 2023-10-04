<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class StrPad implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly int $length,
        private readonly string $pad_string = ' ',
        private readonly int $type = STR_PAD_RIGHT
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $val */
        $val = $this->ref->eval($row);

        if (!\is_string($val)) {
            return null;
        }

        return \str_pad($val, $this->length, $this->pad_string, $this->type);
    }
}
