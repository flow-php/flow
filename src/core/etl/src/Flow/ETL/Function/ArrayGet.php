<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\ArrayDot\array_dot_get;

final class ArrayGet extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction|string $path,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        try {
            $value = (new Parameter($this->ref))->asArray($row, $context);
            $path = (new Parameter($this->path))->asString($row, $context);

            if ($value === null || $path === null) {
                return $context
                    ->functions()
                    ->invalidResult(new InvalidArgumentException('ArrayGet function requires non-null array and path'));
            }

            return array_dot_get($value, $path);
        } catch (InvalidArgumentException $e) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException(
                    'ArrayGet function failed to get value from array.',
                    0,
                    $e,
                ));
        }
    }
}
