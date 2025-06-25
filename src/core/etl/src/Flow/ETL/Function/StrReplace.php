<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_list, type_string, type_union};
use Flow\ETL\Row;

final class StrReplace extends ScalarFunctionChain
{
    /**
     * @param ScalarFunction|string $value
     * @param array<array-key, mixed>|ScalarFunction|string $search
     * @param array<array-key, mixed>|ScalarFunction|string $replace
     */
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

        $typedSearch = type_union(type_string(), type_list(type_string()))->assert($search);
        $typedReplace = type_union(type_string(), type_list(type_string()))->assert($replace);

        return \str_replace($typedSearch, $typedReplace, $value);
    }
}
