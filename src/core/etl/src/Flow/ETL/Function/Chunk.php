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
use Symfony\Component\String\AbstractString;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\s;

final class Chunk implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $size;

    public function __construct(ScalarFunction|string $value, ScalarFunction|int $size)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->size = $size instanceof ScalarFunction ? $size : lit($size);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->size];
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
        return type_list(type_string());
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $context);
        $sizes = (new Parameter($this->size))->asInts($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $size = $sizes[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('Chunk function requires non-null value');
                }

                if ($size === null || $size <= 0) {
                    throw new InvalidArgumentException('Chunk function requires non-null, positive size');
                }

                $chunks = s($value)->chunk($size);

                $results[] = array_map(
                    static fn(AbstractString $chunk): string => $chunk->toString(),
                    iterator_to_array($chunks, false),
                );
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
