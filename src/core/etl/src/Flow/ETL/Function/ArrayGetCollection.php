<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\ArrayDot\array_dot_get;

final class ArrayGetCollection extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed> $keys
     */
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction|array $keys,
        private readonly ScalarFunction|string $index = '*',
    ) {}

    /**
     * @param array<string> $keys
     */
    public static function fromFirst(ScalarFunction $ref, ScalarFunction|array $keys): self
    {
        return new self($ref, $keys, '0');
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        try {
            $value = (new Parameter($this->ref))->asArray($row, $context);
            $index = (new Parameter($this->index))->asString($row, $context);
            $keys = (new Parameter($this->keys))->asArray($row, $context);

            if ($value === null || $index === null || $keys === null) {
                return $context
                    ->functions()
                    ->invalidResult(
                        new InvalidArgumentException(
                            'ArrayGetCollection function requires non-null array, index, and keys',
                        ),
                    );
            }

            $path = \sprintf(
                "{$index}.{%s}",
                \implode(',', \array_map(
                    static fn(mixed $entryName): string => (
                        '?' . (\is_scalar($entryName) ? (string) $entryName : \serialize($entryName))
                    ),
                    $keys,
                )),
            );

            try {
                $array = $index === '0' ? \array_values($value) : $value;

                $extractedValues = array_dot_get($array, $path);
            } catch (InvalidPathException $e) {
                return $context
                    ->functions()
                    ->invalidResult(new InvalidArgumentException(
                        'ArrayGetCollection function failed to get values from array.',
                        0,
                        $e,
                    ));
            }

            if (!\is_array($extractedValues)) {
                return $context
                    ->functions()
                    ->invalidResult(
                        new InvalidArgumentException('ArrayGetCollection function requires the result to be an array'),
                    );
            }

            return $extractedValues;
        } catch (InvalidArgumentException $e) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException(
                    'ArrayGetCollection function failed to evaluate parameters.',
                    0,
                    $e,
                ));
        }
    }
}
