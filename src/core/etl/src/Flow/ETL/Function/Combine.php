<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function array_combine;
use function array_is_list;
use function count;
use function is_int;
use function is_string;

final class Combine extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $keys
     * @param array<array-key, mixed>|ScalarFunction $values
     */
    public function __construct(
        private readonly ScalarFunction|array $keys,
        private readonly ScalarFunction|array $values,
    ) {}

    /**
     * @return null|array<int|string, mixed>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $keys = (new Parameter($this->keys))->asArray($row, $context);
        $values = (new Parameter($this->values))->asArray($row, $context);

        if (null === $keys || null === $values) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Combine function requires non-null arrays'));
        }

        if ([] === $keys) {
            return [];
        }

        if (!array_is_list($keys)) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Combine function requires keys to be a list'));
        }

        if (count($keys) !== count($values)) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException(
                        'Combine function requires keys and values arrays to have the same length',
                    ),
                );
        }

        if (!is_string($keys[0] ?? null) && !is_int($keys[0] ?? null)) {
            return $context
                ->functions()
                ->invalidResult(
                    new InvalidArgumentException('Combine function requires keys to be strings or integers'),
                );
        }

        /** @var array<array-key, array-key> $keys */
        return array_combine($keys, $values);
    }
}
