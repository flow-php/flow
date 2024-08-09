<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class StrReplace extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string|array $search,
        private readonly ScalarFunction|string|array $replace
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asString($row);
        $search = Parameter::oneOf((new Parameter($this->search))->asString($row), (new Parameter($this->search))->asArray($row));
        $replace = Parameter::oneOf((new Parameter($this->replace))->asString($row), (new Parameter($this->replace))->asArray($row));

        if ($value === null || $search === null || $replace === null) {
            return null;
        }

        return \str_replace($search, $replace, $value);
    }
}
