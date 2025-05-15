<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_optional, type_string};
use Flow\ETL\Row;

final class Concat extends ScalarFunctionChain
{
    /**
     * @var array<ScalarFunction|string>
     */
    private readonly array $refs;

    public function __construct(
        ScalarFunction|string ...$refs,
    ) {
        $this->refs = $refs;
    }

    public function eval(Row $row) : string
    {
        /** @var array<string> $concatValues */
        $concatValues = [];

        foreach ($this->refs as $value) {
            $value = \is_string($value) ? $value : type_optional(type_string())->cast((new Parameter($value))->eval($row));

            if (\is_string($value)) {
                $concatValues[] = $value;
            }
        }

        return \implode('', $concatValues);
    }
}
