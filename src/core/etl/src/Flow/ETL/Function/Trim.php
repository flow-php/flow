<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Rows;
use Flow\Types\Type as FlowType;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function ltrim;
use function rtrim;
use function trim;

final class Trim implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;

    private readonly ScalarFunction $type;

    private readonly ScalarFunction $characters;

    public function __construct(
        ScalarFunction|string $value,
        ScalarFunction|Type $type = Type::BOTH,
        ScalarFunction|string $characters = " \t\n\r\0\x0B",
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->type = $type instanceof ScalarFunction ? $type : lit($type);
        $this->characters = $characters instanceof ScalarFunction ? $characters : lit($characters);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->type, $this->characters];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2]);
    }

    /**
     * @return FlowType<mixed>
     */
    public function returns(): FlowType
    {
        return type_string();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->asStrings($rows, $context);
        $types = (new Parameter($this->type))->asEnums($rows, $context, Type::class);
        $charactersList = (new Parameter($this->characters))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $type = $types[$i];
                $characters = $charactersList[$i];

                if ($value === null) {
                    throw new InvalidArgumentException('Trim function requires non-null value');
                }

                if ($type === null || $characters === null) {
                    throw new InvalidArgumentException('Trim function requires non-null type and characters');
                }

                $results[] = match ($type) {
                    Type::LEFT => ltrim($value, $characters),
                    Type::RIGHT => rtrim($value, $characters),
                    Type::BOTH => trim($value, $characters),
                };
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
