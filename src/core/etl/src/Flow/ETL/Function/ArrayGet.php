<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;

use function array_key_exists;
use function explode;
use function Flow\ArrayDot\array_dot_get;
use function Flow\Types\DSL\type_bare;
use function sprintf;
use function str_contains;

final class ArrayGet implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly string $path,
    ) {
        // A wildcard path produces N values from runtime keys - not a single column with one type.
        if (str_contains($path, '*') || str_contains($path, '{')) {
            throw new InvalidArgumentException(sprintf(
                'ArrayGet path "%s" contains a wildcard - a wildcard path cannot describe a single column. Use array_get_collection() instead.',
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
        return new self($children[0], $this->path);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $type = type_bare($this->ref->returns());

        foreach (explode('.', $this->path) as $segment) {
            $bare = type_bare($type);

            if (!$bare instanceof StructureType) {
                throw SchemaNotDerivableException::function(
                    'array_get',
                    'the array operand declares "' . $bare->toString() . '", which is not a structure',
                );
            }

            $elements = $bare->elements() + $bare->optionalElements();

            if (!array_key_exists($segment, $elements)) {
                throw SchemaNotDerivableException::function(
                    'array_get',
                    'path segment "' . $segment . '" is not declared by "' . $bare->toString() . '"',
                );
            }

            $type = $elements[$segment];
        }

        return $type;
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
