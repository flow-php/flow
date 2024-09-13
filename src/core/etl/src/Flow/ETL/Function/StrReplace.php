<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_list, type_string};
use Flow\ETL\Row;

final class StrReplace extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string|array $search,
        private readonly ScalarFunction|string|array $replace,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asString($row);
        $search = (new Parameter($this->search))->as($row, type_string(), type_list(type_string()));
        $replace = (new Parameter($this->replace))->as($row, type_string(), type_list(type_string()));

        if ($value === null || $search === null || $replace === null) {
            return null;
        }

        return \str_replace($search, $replace, $value);
    }
}
