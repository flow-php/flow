<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class StringMatchAll extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $haystack,
        private readonly ScalarFunction|string $pattern,
    ) {
    }

    /**
     * @return array<int, array<int|string, string>>
     */
    public function eval(Row $row) : array
    {
        $haystack = (new Parameter($this->haystack))->asString($row);
        $pattern = (new Parameter($this->pattern))->asString($row);

        if ($haystack === null || $pattern === null) {
            return [];
        }

        try {
            $matches = [];

            if (\preg_match_all($pattern, $haystack, $matches, \PREG_SET_ORDER) !== false) {
                if ($matches === []) {
                    return [];
                }

                /** @var array<int, array<int|string, string>> $result */
                $result = $matches;

                return $result;
            }

            return [];
        } catch (\Throwable) {
            return [];
        }
    }
}
