<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ETL\DSL\lit;
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
        $this->array = $array instanceof ScalarFunction ? $array : lit($array);
        $this->path = $path instanceof ScalarFunction ? $path : lit($path);
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        try {
            $arrays = (new Parameter($this->array))->asArrays($rows, $context);
            $paths = (new Parameter($this->path))->asStrings($rows, $context);
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

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
