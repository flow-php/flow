<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\UnsupportedUnionTypeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidArgumentException as TypesInvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\TypeFactory;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\lit;
use function sprintf;

final class Cast implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var Type<mixed>
     */
    private readonly Type $type;

    private readonly ScalarFunction $value;

    /**
     * The target is resolved and checked here: a string alias becomes a Type, and a type with no
     * Definition arm - not a column - is refused before any row is read.
     *
     * @param string|Type<mixed> $type
     */
    public function __construct(mixed $value, Type|string $type)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);

        try {
            $resolved = $type instanceof Type ? $type : TypeFactory::fromString($type);
            definition_from_type('cast', $resolved);
        } catch (RuntimeException|TypesInvalidArgumentException|UnsupportedUnionTypeException $e) {
            throw new InvalidArgumentException(
                sprintf('Cast function does not support type: %s', $type instanceof Type ? $type->toString() : $type),
                0,
                $e,
            );
        }

        $this->type = $resolved;
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
        return new self($children[0], $this->type);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->type;
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->value))->eval($row, $context);

        if (null === $value) {
            throw new InvalidArgumentException('Cast function requires non-null value');
        }

        try {
            return $this->type->cast($value);
        } catch (CastingException $e) {
            throw new InvalidArgumentException('Cast function failed: ' . $e->getMessage());
        }
    }
}
