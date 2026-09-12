<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\ArrayKey;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\TypeWidener;

use function Flow\ArrayDot\array_dot_get;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_instance_of;
use function sprintf;

final class ArrayGet implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly Path $path;

    public function __construct(
        private readonly ScalarFunction $ref,
        string $path,
    ) {
        try {
            $this->path = Path::fromString($path);
        } catch (InvalidPathException $e) {
            throw new InvalidArgumentException(sprintf('ArrayGet path "%s" is not a valid path.', $path), 0, $e);
        }

        if (!$this->path->selectsSingleValue()) {
            throw new InvalidArgumentException(sprintf(
                'ArrayGet path "%s" selects more than one value - only a path of keys describes a single column. Use array_get_collection() instead.',
                $path,
            ));
        }
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->ref];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->path->toString());
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $type = type_bare($this->ref->returns());
        $nullable = false;

        foreach ($this->path->steps as $step) {
            // the constructor accepts a path of keys only
            $key = type_instance_of(Key::class)->assert($step);
            $bare = type_bare($type);

            if (!$bare instanceof StructureType) {
                throw SchemaNotDerivableException::function(
                    'array_get',
                    'the array operand declares "' . $bare->toString() . '", which is not a structure',
                );
            }

            // '0' finds the element named int 0, exactly as it did when elements were array keys
            $element = $bare->element(ArrayKey::coerce($key->name));

            if ($element === null) {
                throw SchemaNotDerivableException::function(
                    'array_get',
                    'path segment "' . $key->name . '" is not declared by "' . $bare->toString() . '"',
                );
            }

            // a nullsafe step reads null where an optional element is absent
            $nullable = $nullable || $key->nullsafe && $element->optional;
            $type = $element->type;
        }

        return $nullable ? (new TypeWidener())->nullable($type) : $type;
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        try {
            $value = (new Parameter($this->ref))->asArray($row, $context);

            if ($value === null) {
                throw new InvalidArgumentException('ArrayGet function requires non-null array');
            }

            return array_dot_get($value, $this->path);
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('ArrayGet function failed to get value from array.', 0, $e);
        }
    }
}
