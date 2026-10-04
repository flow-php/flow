<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeImmutable;
use DateTimeZone;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_datetime;

final class Now implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $timeZone;

    /**
     * @var DateTimeType<\DateTimeInterface>
     */
    private readonly DateTimeType $type;

    public function __construct(ScalarFunction|DateTimeZone $timeZone = new DateTimeZone('UTC'))
    {
        $this->timeZone = $timeZone instanceof ScalarFunction ? $timeZone : lit($timeZone);
        // @mago-ignore analysis:mixed-assignment
        $zone = $this->timeZone instanceof Literal ? $this->timeZone->value() : null;
        $this->type = $zone instanceof DateTimeZone ? type_datetime($zone) : type_datetime();
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->timeZone];
    }

    public function deterministic(): bool
    {
        return false;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->type;
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $tzes = (new Parameter($this->timeZone))->asInstancesOf($rows, $context, DateTimeZone::class);
        $results = [];
        $i = 0;

        try {
            foreach ($tzes as $i => $tz) {
                if ($tz === null) {
                    throw new InvalidArgumentException('Now function requires valid DateTimeZone');
                }

                $results[] = $this->type->cast(new DateTimeImmutable('now', $tz));
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
