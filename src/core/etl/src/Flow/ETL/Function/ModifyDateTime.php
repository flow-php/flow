<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\OptionalType;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_datetime;

final class ModifyDateTime implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $reference;
    private readonly ScalarFunction $modifier;

    public function __construct(mixed $reference, string|ScalarFunction $modifier)
    {
        $this->reference = $reference instanceof ScalarFunction ? $reference : lit($reference);
        $this->modifier = $modifier instanceof ScalarFunction ? $modifier : lit($modifier);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->reference, $this->modifier];
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
        $type = $this->reference->returns();
        $base = $type instanceof OptionalType ? $type->base() : $type;

        return $base instanceof DateTimeType ? $base : type_datetime();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->reference))->asInstancesOf($rows, $context, DateTimeInterface::class);
        $modifiers = (new Parameter($this->modifier))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $modifier = $modifiers[$i];

                if ($modifier === null || $value === null) {
                    throw new InvalidArgumentException('ModifyDateTime function requires non-null values');
                }

                if (!$value instanceof DateTime && !$value instanceof DateTimeImmutable) {
                    throw new InvalidArgumentException(
                        'ModifyDateTime function requires DateTime or DateTimeImmutable object',
                    );
                }

                $results[] = $value->modify($modifier);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
