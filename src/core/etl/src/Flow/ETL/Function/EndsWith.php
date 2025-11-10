<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class EndsWith extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $haystack,
        private readonly ScalarFunction|string $needle,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : bool
    {
        $haystack = (new Parameter($this->haystack))->asString($row, $context);
        $needle = (new Parameter($this->needle))->asString($row, $context);

        if ($haystack === null || $needle === null) {
            $context->functions()->invalidResult(new InvalidArgumentException('EndsWith function requires non-null haystack and needle'));

            return false;
        }

        return \str_ends_with($haystack, $needle);
    }
}
