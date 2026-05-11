<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function Symfony\Component\String\u;

final class StringAfterLast extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $string,
        private readonly ScalarFunction|string $needle,
        private readonly ScalarFunction|bool $includeNeedle = false,
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $string = (new Parameter($this->string))->asString($row, $context);

        if ($string === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringAfterLast function requires non-null value'));
        }

        $needle = (new Parameter($this->needle))->asString($row, $context) ?? (new Parameter($this->needle))->asArray(
            $row,
            $context,
        );
        $typedNeedle = type_union(type_string(), type_list(type_string()))->assert($needle);
        $includeNeedle = (new Parameter($this->includeNeedle))->asBoolean($row, $context);

        return u($string)->afterLast($typedNeedle, $includeNeedle)->toString();
    }
}
