<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function array_map;
use function array_values;
use function Flow\ArrayDot\array_dot_get;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;
use function implode;
use function is_array;
use function is_scalar;
use function serialize;
use function sprintf;

final class ArrayGetCollection implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $keys;
    private readonly ScalarFunction $index;

    /**
     * @param ScalarFunction|array<array-key, mixed> $keys
     */
    public function __construct(
        private readonly ScalarFunction $ref,
        ScalarFunction|array $keys,
        ScalarFunction|string $index = '*',
    ) {
        $this->keys = $keys instanceof ScalarFunction ? $keys : lit($keys);
        $this->index = $index instanceof ScalarFunction ? $index : lit($index);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->ref, $this->keys, $this->index];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_array());
    }

    /**
     * @param ScalarFunction|array<string> $keys
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
                throw new InvalidArgumentException(
                    'ArrayGetCollection function requires non-null array, index, and keys',
                );
            }

            $path = sprintf(
                "{$index}.{%s}",
                implode(',', array_map(
                    static fn(mixed $entryName): string => (
                        '?' . (is_scalar($entryName) ? (string) $entryName : serialize($entryName))
                    ),
                    $keys,
                )),
            );

            try {
                $array = $index === '0' ? array_values($value) : $value;

                // @mago-ignore analysis:mixed-assignment
                $extractedValues = array_dot_get($array, $path);
            } catch (InvalidPathException $e) {
                throw new InvalidArgumentException(
                    'ArrayGetCollection function failed to get values from array.',
                    0,
                    $e,
                );
            }

            if (!is_array($extractedValues)) {
                throw new InvalidArgumentException('ArrayGetCollection function requires the result to be an array');
            }

            return $extractedValues;
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('ArrayGetCollection function failed to evaluate parameters.', 0, $e);
        }
    }
}
