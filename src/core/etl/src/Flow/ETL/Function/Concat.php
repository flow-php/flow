<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Concat implements ScalarFunction
{
    use EntryScalarFunction;

    /**
     * @var array<ScalarFunction>
     */
    private readonly array $refs;

    public function __construct(
        ScalarFunction ...$refs,
    ) {
        $this->refs = $refs;
    }

    public function eval(Row $row) : mixed
    {
        $values = \array_map(function (ScalarFunction $ref) use ($row) : mixed {
            return (new Cast($ref, 'string'))->eval($row);
        }, $this->refs);

        foreach ($values as $value) {
            if (!\is_string($value)) {
                return null;
            }
        }

        /** @var array<string> $values */
        return \implode('', $values);
    }
}
