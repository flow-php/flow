<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Value\Json;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function gettype;
use function is_scalar;
use function serialize;

final class Hash implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    public function __construct(
        mixed $value,
        private readonly Algorithm $algorithm = new NativePHPHash(),
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->algorithm);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_string());
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->values($rows, $context);
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $i => $value) {
                if ($value instanceof Json) {
                    $value = $value->toArray();
                }

                $results[] = match ($value) {
                    null => null,
                    default => match (gettype($value)) {
                        'array', 'object' => $this->algorithm->hash(serialize($value)),
                        default => $this->algorithm->hash(is_scalar($value) ? (string) $value : ''),
                    },
                };
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
