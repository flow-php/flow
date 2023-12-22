<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class StrPad implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(
        private readonly ScalarFunction $ref,
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
