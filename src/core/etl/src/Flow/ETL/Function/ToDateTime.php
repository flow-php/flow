<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\ToDateTime\PatternCoverage;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_optional;
use function is_int;
use function is_object;
use function is_string;

final class ToDateTime implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $format;
    private readonly ScalarFunction $timeZone;

    /**
     * @var DateTimeType<\DateTimeInterface>
     */
    private readonly DateTimeType $type;

    /**
     * The format as given when it was a plain string; kept across a rebuild that leaves the format child as it is.
     */
    private ?string $pattern;

    public function __construct(
        mixed $value,
        ScalarFunction|string $format,
        ScalarFunction|DateTimeZone $timeZone = new DateTimeZone('UTC'),
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->format = $format instanceof ScalarFunction ? $format : lit($format);
        $this->timeZone = $timeZone instanceof ScalarFunction ? $timeZone : lit($timeZone);
        // @mago-ignore analysis:mixed-assignment
        $zone = $this->timeZone instanceof Literal ? $this->timeZone->value() : null;
        $this->type = $zone instanceof DateTimeZone ? type_datetime($zone) : type_datetime();
        $this->pattern = is_string($format) ? $format : null;
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
        $rebuilt = new self($children[0], $children[1], $children[2]);

        if ($children[1] === $this->format) {
            $rebuilt->pattern = $this->pattern;
        }

        return $rebuilt;
    }

    /**
     * createFromFormat() fills what the format does not parse from the clock; a format only known at run time may.
     */
    public function deterministic(): bool
    {
        if ($this->pattern === null || (new PatternCoverage($this->pattern))->fillsFromClock()) {
            return false;
        }

        foreach ($this->children() as $child) {
            if (!$child->deterministic()) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional($this->type);
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
                    throw new InvalidArgumentException('ToDateTime function requires non-null values');
                }

                if (is_object($value)) {
                    if ($value instanceof DateTimeInterface) {
                        $results[] = $this->type->cast($value);

                        continue;
                    }

                    throw new InvalidArgumentException('ToDateTime function requires DateTimeInterface object');
                }

                if (is_int($value)) {
                    $dateTime = DateTimeImmutable::createFromFormat('U', (string) $value, $timeZone);

                    $results[] = $dateTime === false ? null : $this->type->cast($dateTime);

                    continue;
                }

                if (is_string($value)) {
                    $dateTime = DateTimeImmutable::createFromFormat($format, $value, $timeZone);

                    $results[] = $dateTime === false ? null : $this->type->cast($dateTime);

                    continue;
                }

                throw new InvalidArgumentException('ToDateTime function requires int or string value');
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
