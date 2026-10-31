<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\ToDateTime\PatternCoverage;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use SensitiveParameter;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function is_int;
use function is_object;
use function is_string;

final class ToDateTime implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $format;
    private readonly ScalarFunction $timeZone;

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
        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->format = $format instanceof ScalarFunction ? $format : new Literal($format);
        $this->timeZone = $timeZone instanceof ScalarFunction ? $timeZone : new Literal($timeZone);
        // @mago-ignore analysis:mixed-assignment
        $zone = $this->timeZone instanceof Literal ? $this->timeZone->value() : null;
        $this->type = $zone instanceof DateTimeZone ? type_datetime($zone) : type_datetime();
        $this->pattern = is_string($format) ? $format : null;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            type_optional(type_string())->assert($data['pattern'] ?? null) ?? $rebuild->of(
                $data['format'] ?? null,
                ScalarFunction::class,
            ),
            $rebuild->of($data['time_zone'] ?? null, ScalarFunction::class),
        );
    }

    /**
     * A string pattern is described raw, so the rebuilt function derives it again.
     */
    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'format' => $this->format,
            'time_zone' => $this->timeZone,
            'pattern' => $this->pattern,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->values($rows, $backend);
        $formats = (new Parameter($this->format))->asStrings($rows, $backend);
        $timeZones = (new Parameter($this->timeZone))->asInstancesOf($rows, $backend, DateTimeZone::class);
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $i => $value) {
                $format = $formats[$i];
                $timeZone = $timeZones[$i];

                if ($value === null) {
                    $results[] = null;

                    continue;
                }

                if ($format === null || $timeZone === null) {
                    throw new InvalidArgumentException('ToDateTime function requires a non-null format and time zone');
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}
