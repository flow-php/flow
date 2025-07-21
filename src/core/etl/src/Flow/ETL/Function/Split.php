<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;
use Symfony\Component\String\AbstractString;

final class Split extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $separator,
        private readonly ScalarFunction|int $limit = PHP_INT_MAX,
    ) {
    }

    /**
     * @return null|array<int, string>
     */
    public function eval(Row $row) : ?array
    {
        $value = (new Parameter($this->value))->asString($row);
        $separator = (new Parameter($this->separator))->asString($row);
        $limit = (new Parameter($this->limit))->asInt($row);

        if ($value === null || $separator === null || $limit === null || $separator === '') {
            return null;
        }

        return \array_map(static fn (AbstractString $s) => $s->toString(), s($value)->split($separator, $limit));
    }
}
