<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Sprintf extends ScalarFunctionChain
{
    /**
     * @var array<null|float|int|ScalarFunction|string>
     */
    private array $values;

    public function __construct(
        private readonly ScalarFunction|string $format,
        ScalarFunction|float|int|string|null ...$values,
    ) {
        $this->values = $values;
    }

    public function eval(Row $row) : ?string
    {
        $format = (new Parameter($this->format))->asString($row);

        /**
         * @var array<null|float|int|string> $values
         */
        $values = \array_map(static fn (ScalarFunction|float|int|string|null $value) : mixed => (new Parameter($value))->eval($row), $this->values);

        if ($format === null || \in_array(null, $values, true)) {
            return null;
        }

        return \sprintf($format, ...$values);
    }
}
