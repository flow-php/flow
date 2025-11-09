<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_list, type_string, type_union};
use function Symfony\Component\String\u;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class StringBefore extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string $needle,
        private readonly ScalarFunction|bool $includeNeedle = false,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);

        if ($string === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('StringBefore function requires non-null value'));
        }

        $needle = (new Parameter($this->needle))->as($row, $context, type_string(), type_list(type_string()));
        $typedNeedle = type_union(type_string(), type_list(type_string()))->assert($needle);
        $includeNeedle = (new Parameter($this->includeNeedle))->asBoolean($row, $context);

        return u($string)->before($typedNeedle, $includeNeedle)->toString();
    }
}
