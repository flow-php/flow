<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use BackedEnum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Native\EnumType;
use ReflectionEnum;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class EnumValue implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    public function __construct(mixed $value)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
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
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $value = type_bare($this->value->returns());

        if (!$value instanceof EnumType) {
            throw SchemaNotDerivableException::function(
                'enum_value',
                'the value operand declares "' . $value->toString() . '", which is not an enum',
            );
        }

        $backingType = (new ReflectionEnum($value->class))->getBackingType();

        return $backingType !== null && $backingType->getName() === 'int' ? type_integer() : type_string();
    }

    public function eval(Row $row, FlowContext $context): int|string
    {
        $enum = (new Parameter($this->value))->eval($row, $context);

        if (!$enum instanceof BackedEnum) {
            throw new InvalidArgumentException('EnumValue function requires a BackedEnum value');
        }

        return $enum->value;
    }
}
