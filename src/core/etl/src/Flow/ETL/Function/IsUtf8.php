<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Symfony\Component\String\b;

final class IsUtf8 extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
    ) {}

    public function eval(Row $row, FlowContext $context): bool
    {
        $string = (new Parameter($this->string))->asString($row, $context);

        if ($string === null) {
            $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('IsUtf8 function requires non-null string'));

            return false;
        }

        return b($string)->isUtf8();
    }
}
