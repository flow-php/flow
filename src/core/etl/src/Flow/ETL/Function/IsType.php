<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use Flow\Types\Type\TypeFactory;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function is_string;

final class IsType implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var list<Type<mixed>>
     */
    private readonly array $types;

    private readonly ScalarFunction $value;

    /**
     * @param string|Type<mixed> ...$types
     */
    public function __construct(mixed $value, string|Type ...$types)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->types = array_values(array_map(static fn(string|Type $type): Type => is_string($type)
            ? TypeFactory::fromString($type)
            : $type, $types));
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], ...$this->types);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return (new Nullability())->any(type_boolean(), $this->value->returns());
    }

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $value = (new Parameter($this->value))->eval($row, $context);

        // A NULL operand is unknowable, even against type_null() - ->isNull() is the null test.
        if ($value === null) {
            return null;
        }

        foreach ($this->types as $type) {
            // @mago-ignore analysis:redundant-type-comparison
            if ($type->isValid($value)) {
                return true;
            }
        }

        return false;
    }
}
