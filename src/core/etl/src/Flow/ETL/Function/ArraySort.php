<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Row;

use function is_array;

final class ArraySort extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction|Sort $sortFunction,
        private readonly ScalarFunction|int|null $flags,
        private readonly ScalarFunction|bool $recursive,
    ) {}

    /**
     * @return null|array<mixed>
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->ref))->asArray($row, $context);
        $flags = (new Parameter($this->flags))->asInt($row, $context);
        $recursive = (new Parameter($this->recursive))->asBoolean($row, $context);
        $sortFunction = (new Parameter($this->sortFunction))->asEnum($row, $context, Sort::class);

        if ($array === null || $sortFunction === null) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('ArraySort function requires non-null array and sort function'),
                );
        }

        $this->recursiveSort($array, $sortFunction->value, $flags, $recursive);

        return $array;
    }

    /**
     * @param array<array-key, mixed> $array
     */
    private function recursiveSort(array &$array, callable $function, ?int $flags, bool $recursive): void
    {
        /** @var mixed $value */
        foreach ($array as &$value) {
            if ($recursive && is_array($value)) {
                $this->recursiveSort($value, $function, $flags, true);
            }
        }

        if (null !== $flags) {
            $function($array, $flags);
        } else {
            $function($array);
        }
    }
}
