<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use SensitiveParameter;

use function array_filter;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;

final class ArrayKeep implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $array;
    private readonly ScalarFunction $value;

    /**
     * @param ScalarFunction|array<array-key, mixed> $array
     */
    public function __construct(ScalarFunction|array $array, mixed $value = null)
    {
        $this->array = $array instanceof ScalarFunction ? $array : new Literal($array);
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['array'] ?? null, ScalarFunction::class),
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'array' => $this->array,
            'value' => $this->value,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->array, $this->value];
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
        $array = type_bare($this->array->returns());

        if ($array instanceof ListType) {
            return type_map(type_integer(), $array->element());
        }

        return $array;
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $arrays = (new Parameter($this->array))->asArrays($rows, $backend);
        $values = (new Parameter($this->value))->values($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($arrays as $i => $array) {
                // @mago-ignore analysis:mixed-assignment
                $value = $values[$i];

                if (null === $array) {
                    throw new InvalidArgumentException('ArrayKeep function requires non-null array');
                }

                $results[] = array_filter($array, static fn($item) => $item === $value);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
