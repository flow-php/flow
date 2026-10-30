<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\ScalarFunction\UnpackResults;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;
use SensitiveParameter;

use function count;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;

final class ArrayUnpack implements ScalarFunction, UnpackResults
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $array;

    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     */
    public function __construct(
        ScalarFunction|array $array,
        private readonly Schema $schema,
    ) {
        if (!count($schema)) {
            throw new InvalidArgumentException('array_unpack() requires at least one declared column');
        }

        $this->array = $array instanceof ScalarFunction ? $array : new Literal($array);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['array'] ?? null, ScalarFunction::class),
            Schema::fromArray(type_array()->assert($data['schema'] ?? null)),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['array' => $this->array, 'schema' => $this->schema->normalize()]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->array];
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $results = [];
        $i = 0;

        try {
            foreach ((new Parameter($this->array))->asArrays($rows, $backend) as $i => $array) {
                $results[] = $array ?? throw new InvalidArgumentException('array_unpack() requires a non-null array');
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $elements = [];

        // every declared column is nullable - a payload that omits a key still has to produce it
        foreach ($this->schema->definitions() as $definition) {
            $name = $definition->name();
            $elements[$name] = structure_element($name, type_optional($definition->type()));
        }

        return type_structure($elements);
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->schema);
    }
}
