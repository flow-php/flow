<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function in_array;

final class IsIn extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $haystack
     * @param mixed $needle
     */
    public function __construct(
        private readonly ScalarFunction|array $haystack,
        private readonly mixed $needle,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $haystack = (new Parameter($this->haystack))->asArray($row, $context);

        if ($haystack === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('IsIn function requires non-null array'));
        }

        return in_array((new Parameter($this->needle))->eval($row, $context), $haystack, true);
    }
}
