<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Throwable;

use function count;
use function Symfony\Component\String\s;

final class StringMatch extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $haystack,
        private readonly ScalarFunction|string $pattern,
    ) {}

    /**
     * @return null|array<int|string, string>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $haystack = (new Parameter($this->haystack))->asString($row, $context);
        $pattern = (new Parameter($this->pattern))->asString($row, $context);

        if ($haystack === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringMatch function requires non-null haystack'));
        }

        if ($pattern === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringMatch function requires non-null pattern'));
        }

        try {
            /** @var array<int|string, string> $result */
            $result = s($haystack)->match($pattern);

            return count($result) > 0 ? $result : null;
        } catch (Throwable $e) {
            $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringMatch error: ' . $e->getMessage()));

            return null;
        }
    }
}
