<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTime;
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

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_date;
use function is_a;
use function is_int;
use function is_object;
use function is_string;

final class ToDate implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $format;
    private readonly ScalarFunction $timeZone;

    public function __construct(
        mixed $value,
        ScalarFunction|string $format,
        ScalarFunction|DateTimeZone $timeZone = new DateTimeZone('UTC'),
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->format = $format instanceof ScalarFunction ? $format : lit($format);
        $this->timeZone = $timeZone instanceof ScalarFunction ? $timeZone : lit($timeZone);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->format, $this->timeZone];
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
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_date();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->values($rows, $context);
        $formats = (new Parameter($this->format))->asStrings($rows, $context);
        $timeZones = (new Parameter($this->timeZone))->asInstancesOf($rows, $context, DateTimeZone::class);
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $i => $value) {
                $format = $formats[$i];
                $timeZone = $timeZones[$i];

                if ($value === null || $format === null || $timeZone === null) {
                    throw new InvalidArgumentException('ToDate function requires non-null values');
                }

                if (is_object($value)) {
                    if (is_a($value, DateTimeImmutable::class) || is_a($value, DateTime::class)) {
                        $results[] = $value->setTimezone($timeZone)->setTime(0, 0, 0, 0);

                        continue;
                    }

                    throw new InvalidArgumentException('ToDate function requires DateTimeInterface object');
                }

                if (is_int($value)) {
                    $date = DateTimeImmutable::createFromFormat('U', (string) $value, $timeZone);

                    $results[] = $date === false ? null : $date->setTime(0, 0, 0, 0);

                    continue;
                }

                if (is_string($value)) {
                    $date = DateTimeImmutable::createFromFormat($format, $value, $timeZone);

                    $results[] = $date === false ? null : $date->setTime(0, 0, 0, 0);

                    continue;
                }

                throw new InvalidArgumentException('ToDate function requires int or string value');
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
