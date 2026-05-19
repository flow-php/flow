<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function in_array;
use function is_array;
use function is_string;
use function str_contains;

final class Contains extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $haystack,
        private readonly ScalarFunction|string $needle,
    ) {}

    public function eval(Row $row, FlowContext $context): bool
    {
        $haystack = (new Parameter($this->haystack))->as($row, $context, type_string(), type_array());
        $needle = (new Parameter($this->needle))->asString($row, $context);

        if ($haystack === null || $needle === null) {
            $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('Contains function requires non-null haystack and needle'),
                );

            return false;
        }

        if (is_string($haystack)) {
            return str_contains($haystack, $needle);
        }

        if (is_array($haystack)) {
            return in_array($needle, $haystack, true);
        }

        $context
            ->functions()
            ->invalidResult(new InvalidArgumentException('Contains function requires haystack to be string or array'));

        return false;
    }
}
