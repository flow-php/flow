<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Symfony\Component\String\AbstractString;

use function array_map;
use function Symfony\Component\String\s;

final class Split extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $separator,
        private readonly ScalarFunction|int $limit = PHP_INT_MAX,
    ) {}

    /**
     * @return null|array<int, string>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $separator = (new Parameter($this->separator))->asString($row, $context);
        $limit = (new Parameter($this->limit))->asInt($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Split function requires non-null value'));
        }

        if ($separator === null || $limit === null || $separator === '') {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException(
                        'Split function requires non-null separator and limit, separator cannot be empty',
                    ),
                );
        }

        return array_map(static fn(AbstractString $s) => $s->toString(), s($value)->split($separator, $limit));
    }
}
