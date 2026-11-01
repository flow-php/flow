<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ArraySort\Sort;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function array_values;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_string;
use function is_array;
use function krsort;
use function ksort;

final class ArraySort implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $flags;
    private readonly ScalarFunction $recursive;

    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly Sort $sortFunction,
        ScalarFunction|int|null $flags,
        ScalarFunction|bool $recursive,
    ) {
        $this->flags = $flags instanceof ScalarFunction ? $flags : new Literal($flags);
        $this->recursive = $recursive instanceof ScalarFunction ? $recursive : new Literal($recursive);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['ref'] ?? null, ScalarFunction::class),
            type_enum(Sort::class)->assert(constant(
                Sort::class . '::' . type_string()->assert($data['sort_function'] ?? null),
            )),
            $rebuild->of($data['flags'] ?? null, ScalarFunction::class),
            $rebuild->of($data['recursive'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'ref' => $this->ref,
            'sort_function' => $this->sortFunction->name,
            'flags' => $this->flags,
            'recursive' => $this->recursive,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->ref, $this->flags, $this->recursive];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->sortFunction, $children[1], $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $array = type_bare($this->ref->returns());

        // sorting by array key reproduces PHP's key order for integer element names
        if (
            $array instanceof StructureType
            && ($this->sortFunction === Sort::ksort || $this->sortFunction === Sort::krsort)
        ) {
            $byName = [];

            foreach ($array->elements() as $element) {
                $byName[$element->name] = $element;
            }

            if ($this->sortFunction === Sort::ksort) {
                ksort($byName);
            } else {
                krsort($byName);
            }

            return new StructureType(array_values($byName));
        }

        return $array;
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $arrays = (new Parameter($this->ref))->asArrays($rows, $backend);
        $flagsList = (new Parameter($this->flags))->asInts($rows, $backend);
        $recursives = (new Parameter($this->recursive))->asBooleans($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($arrays as $i => $array) {
                $flags = $flagsList[$i];
                $recursive = $recursives[$i];

                $recursive ??= false;

                if ($array === null) {
                    throw new InvalidArgumentException('ArraySort function requires non-null array');
                }

                $this->recursiveSort($array, $this->sortFunction->value, $flags, $recursive);

                $results[] = $array;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }

    /**
     * @param array<array-key, mixed> $array
     */
    private function recursiveSort(array &$array, callable $function, ?int $flags, bool $recursive): void
    {
        /** @var mixed $value */
        foreach ($array as &$value) {
            if ($recursive && is_array($value)) {
                $this->recursiveSort($value, $function, $flags, true);
            }
        }

        if (null !== $flags) {
            $function($array, $flags);
        } else {
            $function($array);
        }
    }
}
