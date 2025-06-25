<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Function\MatchCases\MatchCondition;
use Flow\ETL\Row;

final class MatchCases extends ScalarFunctionChain
{
    /**
     * @param array<MatchCondition> $cases
     * @param mixed $default
     */
    public function __construct(private readonly array $cases, private readonly mixed $default = null)
    {

    }

    public function eval(Row $row) : mixed
    {
        foreach ($this->cases as $condition) {
            if ($condition->valid($row)) {
                return $condition->eval($row);
            }
        }

        if ($this->default) {
            return (new Parameter($this->default))->eval($row);
        }

        throw new RuntimeException(
            'Not a single case matches row, consider using default parameter, row: '
            . \json_encode($row->toArray(), JSON_THROW_ON_ERROR)
        );
    }
}
