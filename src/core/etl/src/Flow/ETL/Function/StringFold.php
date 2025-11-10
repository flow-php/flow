<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\u;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class StringFold extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|string $string)
    {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);

        if ($string === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('StringFold function requires non-null value'));
        }

        return u($string)->folded()->toString();
    }
}
