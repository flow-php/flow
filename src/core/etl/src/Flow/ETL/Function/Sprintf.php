<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Sprintf implements ScalarFunction
{
    use EntryScalarFunction;

    /**
     * @var array<ScalarFunction>
     */
    private array $values;

    public function __construct(
        private readonly ScalarFunction $format,
        ScalarFunction ...$values
    ) {
        $this->values = $values;
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     */
    public function eval(Row $row) : ?string
    {
        $format = $this->format->eval($row);
        /**
         * @var array<null|float|int|string> $values
         */
        $values = \array_map(static fn (ScalarFunction $value) : mixed => $value->eval($row), $this->values);

        if (!\is_string($format) || \in_array(null, $values, true)) {
            return null;
        }

        return \sprintf($format, ...$values);
    }
}
