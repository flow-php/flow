<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class Capitalize extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $string = (new Parameter($this->string))->eval($row, $context);

        if ($string === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Capitalize function requires non-null value'));
        }

        if (\function_exists('mb_convert_case')) {
            return \mb_convert_case(\is_scalar($string) ? (string) $string : '', \MB_CASE_TITLE);
        }

        return \ucwords(\is_scalar($string) ? (string) $string : '');
    }
}
