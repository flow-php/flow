<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class ArrayReverse extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     * @param bool|ScalarFunction $preserveKeys
     */
    public function __construct(
        private readonly ScalarFunction|array $array,
        private readonly ScalarFunction|bool $preserveKeys,
    ) {}

    /**
     * @return null|array<mixed>
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->array))->asArray($row, $context);
        $preserveKeys = (new Parameter($this->preserveKeys))->asBoolean($row, $context);

        if ($array === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('ArrayReverse function requires non-null array'));
        }

        return \array_reverse($array, $preserveKeys);
    }
}
