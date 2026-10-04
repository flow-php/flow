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

use function Flow\ArrayDot\array_dot_rename;
use function Flow\Types\DSL\type_array;

final class ArrayKeyRename implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly string $path,
        private readonly string $newName,
    ) {}

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
        return new self($children[0], $this->path, $this->newName);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_array();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->ref))->asArrays($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                if ($value === null) {
                    throw new InvalidArgumentException('ArrayKeyRename function requires non-null array');
                }

                $results[] = array_dot_rename($value, $this->path, $this->newName);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
