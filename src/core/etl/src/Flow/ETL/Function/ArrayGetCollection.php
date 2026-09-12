<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use Flow\ArrayDot\Step\Multimatch;
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
use function get_debug_type;
use function is_array;
use function is_scalar;
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

            // the keys are literal, so they become Key steps and are never parsed as a path
            $keyPaths = array_map(static function (mixed $key): Path {
                if (!is_scalar($key)) {
                    throw new InvalidArgumentException(sprintf(
                        'ArrayGetCollection keys must be scalar, got "%s".',
                        get_debug_type($key),
                    ));
                }

                return new Path([new Key((string) $key, nullsafe: true)]);
            }, array_values($keys));

            try {
                $array = $index === '0' ? array_values($value) : $value;

                // @mago-ignore analysis:mixed-assignment
                $extractedValues = array_dot_get($array, new Path([
                    ...Path::fromString($index)->steps,
                    new Multimatch($keyPaths),
                ]));
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
