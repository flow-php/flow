<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;

use function array_combine;
use function array_is_list;
use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_map;
use function is_int;
use function is_string;

final class Combine implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @param array<array-key, mixed>|ScalarFunction $keys
     * @param array<array-key, mixed>|ScalarFunction $values
     */
    private readonly ScalarFunction $keys;
    private readonly ScalarFunction $values;

    public function __construct(ScalarFunction|array $keys, ScalarFunction|array $values)
    {
        $this->keys = $keys instanceof ScalarFunction ? $keys : lit($keys);
        $this->values = $values instanceof ScalarFunction ? $values : lit($values);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->keys, $this->values];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $keys = type_bare($this->keys->returns());
        $values = type_bare($this->values->returns());

        if (!$keys instanceof ListType || !$values instanceof ListType) {
            throw SchemaNotDerivableException::function('combine', 'both operands must declare list types');
        }

        // U-04a.11: a key type outside the array-key template is refused at the Definition boundary.
        // @mago-expect analysis:template-constraint-violation
        // @mago-expect analysis:less-specific-nested-argument-type
        return type_map($keys->element(), $values->element());
    }

    /**
     * @return null|array<int|string, mixed>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $keys = (new Parameter($this->keys))->asArray($row, $context);
        $values = (new Parameter($this->values))->asArray($row, $context);

        if (null === $keys || null === $values) {
            throw new InvalidArgumentException('Combine function requires non-null arrays');
        }

        if ([] === $keys) {
            return [];
        }

        if (!array_is_list($keys)) {
            throw new InvalidArgumentException('Combine function requires keys to be a list');
        }

        if (count($keys) !== count($values)) {
            throw new InvalidArgumentException(
                'Combine function requires keys and values arrays to have the same length',
            );
        }

        if (!is_string($keys[0] ?? null) && !is_int($keys[0] ?? null)) {
            throw new InvalidArgumentException('Combine function requires keys to be strings or integers');
        }

        /** @var array<array-key, array-key> $keys */
        return array_combine($keys, $values);
    }
}
