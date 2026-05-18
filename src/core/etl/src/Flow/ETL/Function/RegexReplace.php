<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function preg_replace;

final class RegexReplace extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $pattern,
        private readonly ScalarFunction|string $replacement,
        private readonly ScalarFunction|string $subject,
        private readonly ScalarFunction|int|null $limit = null,
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $pattern = (new Parameter($this->pattern))->asString($row, $context);
        $replacement = (new Parameter($this->replacement))->asString($row, $context);
        $subject = (new Parameter($this->subject))->asString($row, $context);
        $limit = $this->limit ? (new Parameter($this->limit))->asInt($row, $context) : -1;

        if ($pattern === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('RegexReplace requires non-null pattern'));
        }

        if ($replacement === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('RegexReplace requires non-null replacement'));
        }

        if ($subject === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('RegexReplace requires non-null subject'));
        }

        if ($limit === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('RegexReplace requires non-null limit'));
        }

        return preg_replace($pattern, $replacement, $subject, $limit);
    }
}
