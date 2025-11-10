<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Function\MatchCases\MatchCondition;

final class MatchCases extends ScalarFunctionChain
{
    /**
     * @param array<MatchCondition> $cases
     * @param mixed $default
     */
    public function __construct(private readonly array $cases, private readonly mixed $default = null)
    {

    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        foreach ($this->cases as $condition) {
            if ($condition->valid($row, $context)) {
                return $condition->eval($row, $context);
            }
        }

        if ($this->default) {
            return (new Parameter($this->default))->eval($row, $context);
        }

        return $context->functions()->invalidResult(
            new InvalidArgumentException(
                'Not a single case matches row, consider using default parameter, row: '
                . \json_encode($row->toArray(), JSON_THROW_ON_ERROR)
            )
        );
    }
}
