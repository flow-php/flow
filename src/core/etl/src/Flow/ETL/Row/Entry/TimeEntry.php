<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use DateInterval;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\TimeDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;
use ReflectionProperty;

use function Flow\ETL\DSL\date_interval_to_microseconds;
use function Flow\Types\DSL\type_equals;
use function json_encode;

/**
 * @template-covariant T of \DateInterval|null
 *
 * @implements Entry<T>
 */
final class TimeEntry implements Entry
{
    use EntryRef;

    private TimeDefinition $definition;

    /**
     * @param T $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?DateInterval $value,
        ?Metadata $metadata = null,
    ) {
        if ($name === '') {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if ($value !== null && ($value->y !== 0 || $value->m !== 0)) {
            throw new InvalidArgumentException(
                "Relative DateInterval (with months/years) can't be converted to TimeEntry. Given"
                    . json_encode($value, JSON_THROW_ON_ERROR),
            );
        }

        $this->definition = new TimeDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    /**
     * @return self<DateInterval>
     */
    public static function fromDays(string $name, int $days): self
    {
        return new self($name, new DateInterval('P' . $days . 'D'));
    }

    /**
     * @return self<DateInterval>
     */
    public static function fromHours(string $name, int $hours): self
    {
        return new self($name, new DateInterval('PT' . $hours . 'H'));
    }

    /**
     * @return self<DateInterval>
     */
    public static function fromMicroseconds(string $name, int $microseconds): self
    {
        $seconds = intdiv($microseconds, 1_000_000);
        $fraction = ($microseconds % 1_000_000) / 1_000_000;

        $interval = new DateInterval('PT' . $seconds . 'S');
        (new ReflectionProperty($interval, 'f'))->setValue($interval, $fraction);

        return new self($name, $interval);
    }

    /**
     * @return self<DateInterval>
     */
    public static function fromMilliseconds(string $name, int $milliseconds): self
    {
        $seconds = intdiv($milliseconds, 1000);
        $fraction = ($milliseconds % 1000) / 1000;

        $interval = new DateInterval('PT' . $seconds . 'S');
        (new ReflectionProperty($interval, 'f'))->setValue($interval, $fraction);

        return new self($name, $interval);
    }

    /**
     * @return self<DateInterval>
     */
    public static function fromMinutes(string $name, int $minutes): self
    {
        return new self($name, new DateInterval('PT' . $minutes . 'M'));
    }

    /**
     * @return self<DateInterval>
     */
    public static function fromSeconds(string $name, int $seconds): self
    {
        return new self($name, new DateInterval('PT' . $seconds . 'S'));
    }

    /**
     * @return self<DateInterval>
     */
    public static function fromString(string $name, string $time): self
    {
        return new self($name, new DateInterval($time));
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): TimeDefinition
    {
        return $this->definition;
    }

    public function is(string|Reference $name): bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry): bool
    {
        if (!$entry instanceof self || !$this->is($entry->name()) || !type_equals($this->type(), $entry->type())) {
            return false;
        }

        $entryValue = $entry->value();
        $thisValue = $this->value();

        if ($entryValue === null && $thisValue === null) {
            return true;
        }

        if ($entryValue === null || $thisValue === null) {
            return false;
        }

        return date_interval_to_microseconds($thisValue) == date_interval_to_microseconds($entryValue);
    }

    public function name(): string
    {
        return $this->name;
    }

    public function rename(string $name): static
    {
        return new self($name, $this->value, $this->definition->metadata());
    }

    public function toString(): string
    {
        $value = $this->value;

        if ($value === null) {
            return '';
        }

        $totalHours = ($value->d * 24) + $value->h; // Convert days to hours and add to hours

        if ($value->f && $value->f > 0) {
            return sprintf('%02d:%02d:%02d.%06d', $totalHours, $value->i, $value->s, $value->f * 1e6);
        }

        return sprintf('%02d:%02d:%02d', $totalHours, $value->i, $value->s);
    }

    /**
     * @return Type<DateInterval>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?DateInterval
    {
        return $this->value;
    }
}
