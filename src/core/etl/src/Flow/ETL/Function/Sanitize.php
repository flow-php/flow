<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Sanitize extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $placeholder,
        private readonly ScalarFunction|int|null $skipCharacters = null
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $val = (new Parameter($this->value))->asString($row);
        $placeholder = (new Parameter($this->placeholder))->asString($row);
        $skipCharacters = (new Parameter($this->skipCharacters))->asInt($row);

        if ($val === null || $placeholder === null) {
            return null;
        }

        $size = \mb_strlen($val);

        if ($skipCharacters !== null && $size > $skipCharacters) {
            return \mb_substr($val, 0, $skipCharacters) . \str_repeat($placeholder, $size - $skipCharacters);
        }

        return \str_repeat($placeholder, $size);
    }
}
