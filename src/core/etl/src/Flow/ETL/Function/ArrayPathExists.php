<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ArrayDot\array_dot_exists;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class ArrayPathExists extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     * @param ScalarFunction|string $path
     */
    public function __construct(
        private readonly ScalarFunction|array $array,
        private readonly ScalarFunction|string $path,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : bool
    {
        try {
            $array = (new Parameter($this->array))->asArray($row, $context);
            $path = (new Parameter($this->path))->asString($row, $context);

            if ($array === null || $path === null) {
                $context->functions()->invalidResult(new InvalidArgumentException('ArrayPathExists function requires non-null array and path'));

                return false;
            }

            return array_dot_exists($array, $path);
        } catch (InvalidArgumentException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('ArrayPathExists error: ' . $e->getMessage()));

            return false;
        }
    }
}
