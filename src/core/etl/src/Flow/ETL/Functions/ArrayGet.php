<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\ArrayKey;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\TypeWidener;
use SensitiveParameter;

use function Flow\ArrayDot\array_dot_get;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
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

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['ref'] ?? null, ScalarFunction::class),
            type_string()->assert($data['path'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['ref' => $this->ref, 'path' => $this->path->toString()]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        try {
            $values = (new Parameter($this->ref))->asArrays($rows, $backend);
        } catch (EvaluationException $e) {
            throw new EvaluationException(
                $e->rowIndex,
                new InvalidArgumentException('ArrayGet function failed to get value from array.', 0, $e),
            );
        }

        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                try {
                    if ($value === null) {
                        throw new InvalidArgumentException('ArrayGet function requires non-null array');
                    }

                    $results[] = array_dot_get($value, $this->path);
                } catch (InvalidArgumentException $e) {
                    throw new InvalidArgumentException('ArrayGet function failed to get value from array.', 0, $e);
                }
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
