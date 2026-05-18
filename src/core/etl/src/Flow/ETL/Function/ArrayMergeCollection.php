<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function array_merge;
use function array_values;
use function is_array;

final class ArrayMergeCollection extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     */
    public function __construct(
        private readonly ScalarFunction|array $array,
    ) {}

    /**
     * @return null|array<mixed>
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->array))->asArray($row, $context);

        if ($array === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('ArrayMergeCollection function requires non-null array'));
        }

        foreach ($array as $element) {
            if (!is_array($element)) {
                return $context
                    ->functions()
                    ->invalidResult(
                        new InvalidArgumentException(
                            'ArrayMergeCollection function requires array elements to be arrays',
                        ),
                    );
            }
        }

        /** @var array<array<mixed>> $array */
        return array_merge(...array_values($array));
    }
}
