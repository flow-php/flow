<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Statistics;

final readonly class HighResolutionTime implements \Stringable
{
    /**
     * @param int $seconds
     * @param int $nanoseconds
     */
    public function __construct(public int $seconds, public int $nanoseconds)
    {
    }

    public static function now() : self
    {
        $timeParts = \hrtime(as_number: false);

        return new self($timeParts[0], $timeParts[1]);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function diff(self $other) : self
    {
        $diffSeconds = $other->seconds - $this->seconds;
        $diffNanoseconds = $other->nanoseconds - $this->nanoseconds;

        // Adjust for negative nanoseconds
        if ($diffNanoseconds < 0) {
            $diffNanoseconds += 1_000_000_000;
            $diffSeconds--;
        }

        return new self($diffSeconds, $diffNanoseconds);
    }

    /**
     * @return array<int>
     */
    public function toArray() : array
    {
        return [$this->seconds, $this->nanoseconds];
    }

    public function toSeconds() : float
    {
        return $this->seconds + $this->nanoseconds / 1_000_000_000;
    }

    public function toString(int $precision = 9) : string
    {
        $formatted = number_format($this->toSeconds(), $precision, '.', '');
        $formatted = rtrim($formatted, '0');
        $formatted = rtrim($formatted, '.');

        return $formatted . 's';
    }
}
