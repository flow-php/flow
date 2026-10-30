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
use Flow\Types\Type\Nullability;
use SensitiveParameter;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\Types\DSL\type_boolean;

final class ArrayPathExists implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $array;
    private readonly ScalarFunction $path;

    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     */
    public function __construct(ScalarFunction|array $array, ScalarFunction|string $path)
    {
        $this->array = $array instanceof ScalarFunction ? $array : new Literal($array);
        $this->path = $path instanceof ScalarFunction ? $path : new Literal($path);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['array'] ?? null, ScalarFunction::class),
            $rebuild->of($data['path'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'array' => $this->array,
            'path' => $this->path,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->array, $this->path];
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
        return (new Nullability())->any(type_boolean(), $this->array->returns(), $this->path->returns());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        try {
            $arrays = (new Parameter($this->array))->asArrays($rows, $backend);
            $paths = (new Parameter($this->path))->asStrings($rows, $backend);
        } catch (EvaluationException $e) {
            throw new EvaluationException(
                $e->rowIndex,
                new InvalidArgumentException(
                    'ArrayPathExists error: ' . ($e->getPrevious()?->getMessage() ?? $e->getMessage()),
                ),
            );
        }

        $results = [];
        $i = 0;

        try {
            foreach ($arrays as $i => $array) {
                $path = $paths[$i];

                try {
                    if ($array === null || $path === null) {
                        $results[] = null;

                        continue;
                    }

                    $results[] = array_dot_exists($array, $path);
                } catch (InvalidArgumentException $e) {
                    throw new InvalidArgumentException('ArrayPathExists error: ' . $e->getMessage(), 0, $e);
                }
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
