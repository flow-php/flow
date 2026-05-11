<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Symfony\Component\String\u;

final class StringTitle extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|bool $allWords = false,
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);
        $allWords = (new Parameter($this->allWords))->asBoolean($row, $context);

        if ($string === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringTitle function requires non-null value'));
        }

        return u($string)->title(allWords: $allWords)->toString();
    }
}
