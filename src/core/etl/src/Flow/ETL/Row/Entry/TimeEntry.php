<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\{date_interval_to_microseconds, type_time};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\TimeType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Entry, Reference};

/**
 * @implements Entry<?\DateInterval, \DateInterval>
 */
final class TimeEntry implements Entry
{
    use EntryRef;

    private readonly TimeType $type;

    /**
     * Time represented php \DateInterval.
     *
     * @var null|\DateInterval
     */
    private readonly ?\DateInterval $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, \DateInterval|string|null $value)
    {
        if ($name === '') {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if ($value instanceof \DateInterval) {
            if ($value->y !== 0 || $value->m !== 0) {
                throw new InvalidArgumentException("Relative DateInterval (with months/years) can't be converted to TimeEntry. Given" . \json_encode($value, JSON_THROW_ON_ERROR));
            }

            $this->value = $value;
        } elseif (\is_string($value)) {
            try {
                $interval = new \DateInterval($value);

                if ($interval->y !== 0 || $interval->m !== 0) {
                    throw new InvalidArgumentException("Relative DateInterval (with months/years) can't be converted to microseconds. Given" . \json_encode($interval, JSON_THROW_ON_ERROR));
                }

                $this->value = $interval;
            } catch (\Throwable $dateIntervalException) {
                try {
                    $dateTime = new \DateTimeImmutable($value);

                    // Get hours, minutes, seconds, and fractional seconds
                    $hours = (int) $dateTime->format('H');
                    $minutes = (int) $dateTime->format('i');
                    $seconds = (int) $dateTime->format('s');
                    $fraction = (float) ('0.' . $dateTime->format('u')); // Microseconds as fractional part

                    // Construct the DateInterval
                    $interval = new \DateInterval('PT' . $hours . 'H' . $minutes . 'M' . $seconds . 'S');
                    $interval->f = $fraction; // Set the fractional seconds

                    if ($interval->y !== 0 || $interval->m !== 0) {
                        throw new InvalidArgumentException("Relative DateInterval (with months/years) can't be converted to microseconds. Given" . \json_encode($interval, JSON_THROW_ON_ERROR));
                    }

                    $this->value = $interval;
                } catch (\Throwable $dateTimeException) {
                    throw $dateIntervalException;
                }
            }
        } else {
            $this->value = null;
        }

        $this->type = type_time($this->value === null);
    }

    public static function fromDays(string $name, int $days) : self
    {
        return new self($name, 'P' . $days . 'D');
    }

    public static function fromHours(string $name, int $hours) : self
    {
        return new self($name, 'PT' . $hours . 'H');
    }

    public static function fromMicroseconds(string $name, int $microseconds) : self
    {
        $seconds = intdiv($microseconds, 1_000_000);
        $fraction = ($microseconds % 1_000_000) / 1_000_000;

        $interval = new \DateInterval('PT' . $seconds . 'S');
        $interval->f = $fraction;

        return new self($name, $interval);
    }

    public static function fromMilliseconds(string $name, int $milliseconds) : self
    {
        $seconds = intdiv($milliseconds, 1000);
        $fraction = ($milliseconds % 1000) / 1000;

        $interval = new \DateInterval('PT' . $seconds . 'S');
        $interval->f = $fraction;

        return new self($name, $interval);
    }

    public static function fromMinutes(string $name, int $minutes) : self
    {
        return new self($name, 'PT' . $minutes . 'M');
    }

    public static function fromSeconds(string $name, int $seconds) : self
    {
        return new self($name, 'PT' . $seconds . 'S');
    }

    public static function fromString(string $name, string $time) : self
    {
        return new self($name, $time);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return Definition::dateTime($this->name, $this->type->nullable());
    }

    public function is(string|Reference $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        $entryValue = $entry->value();
        $thisValue = $this->value();

        if ($entryValue === null && $thisValue === null) {
            return true;
        }

        if ($entryValue === null || $thisValue === null) {
            return false;
        }

        return $this->is($entry->name())
            && $entry instanceof self
            && $this->type->isEqual($entry->type)
            && date_interval_to_microseconds($thisValue) == date_interval_to_microseconds($entryValue);
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    public function toString() : string
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

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?\DateInterval
    {
        return $this->value;
    }

    public function withValue(mixed $value) : Entry
    {
        return new self($this->name, $value);
    }
}
