<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_list, type_string};
use function Symfony\Component\String\s;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class StringContainsAny extends ScalarFunctionChain
{
    /**
     * @param ScalarFunction|string $value
     * @param array<string>|ScalarFunction $needles
     */
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|array $needles,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : bool
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $needles = (new Parameter($this->needles))->as($row, $context, type_list(type_string()));

        if ($value === null) {
            $context->functions()->invalidResult(new InvalidArgumentException('StringContainsAny function requires non-null string'));

            return false;
        }

        if ($needles === null || !\is_array($needles) || \count($needles) === 0) {
            $context->functions()->invalidResult(new InvalidArgumentException('StringContainsAny function requires non-null, non-empty needles array'));

            return false;
        }

        return s($value)->containsAny($needles);
    }
}
