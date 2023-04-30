<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Sprintf implements Expression
{
    /**
     * @var array<Expression>
     */
    private array $values;

    public function __construct(
        private readonly Expression $format,
        Expression ...$values
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
        $values = \array_map(static fn (Expression $value) : mixed => $value->eval($row), $this->values);

        if (!\is_string($format) || \in_array(null, $values, true)) {
            return null;
        }

        return \sprintf($format, ...$values);
    }
}
