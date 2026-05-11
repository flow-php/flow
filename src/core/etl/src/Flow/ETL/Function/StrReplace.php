<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

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
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $search = (new Parameter($this->search))->asString($row, $context) ?? (new Parameter($this->search))->asArray(
            $row,
            $context,
        );
        $replace = (new Parameter($this->replace))->asString(
            $row,
            $context,
        ) ?? (new Parameter($this->replace))->asArray($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StrReplace function requires non-null value'));
        }

        if ($search === null || $replace === null) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('StrReplace function requires non-null search and replace'),
                );
        }

        $typedSearch = type_union(type_string(), type_list(type_string()))->assert($search);
        $typedReplace = type_union(type_string(), type_list(type_string()))->assert($replace);

        return \str_replace($typedSearch, $typedReplace, $value);
    }
}
