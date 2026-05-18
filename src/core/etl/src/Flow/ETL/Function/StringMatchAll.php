<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Throwable;

use function preg_match_all;

use const PREG_SET_ORDER;

final class StringMatchAll extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $haystack,
        private readonly ScalarFunction|string $pattern,
    ) {}

    /**
     * @return null|array<int, array<int|string, string>>
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        $haystack = (new Parameter($this->haystack))->asString($row, $context);
        $pattern = (new Parameter($this->pattern))->asString($row, $context);

        if ($haystack === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringMatchAll function requires non-null haystack'));
        }

        if ($pattern === null) {
            $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringMatchAll function requires non-null pattern'));

            return [];
        }

        try {
            $matches = [];

            if (preg_match_all($pattern, $haystack, $matches, PREG_SET_ORDER) !== false) {
                return $matches;
            }

            return [];
        } catch (Throwable $e) {
            $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringMatchAll error: ' . $e->getMessage()));

            return [];
        }
    }
}
