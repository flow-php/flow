<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class StringMatch extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $haystack,
        private readonly ScalarFunction|string $pattern,
    ) {
    }

    /**
     * @return null|array<int|string, string>
     */
    public function eval(Row $row) : ?array
    {
        $haystack = (new Parameter($this->haystack))->asString($row);
        $pattern = (new Parameter($this->pattern))->asString($row);

        if ($haystack === null || $pattern === null) {
            return null;
        }

        try {
            /** @var array<int|string, string> $result */
            $result = s($haystack)->match($pattern);

            return \count($result) > 0 ? $result : null;
        } catch (\Throwable) {
            return null;
        }
    }
}
