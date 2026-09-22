<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;

final readonly class Cardinality
{
    public const float DEFAULT_RELATIVE_ERROR = 0.5;

    /**
     * @var null|int<0, max>
     */
    public ?int $atMost;

    /**
     * @var null|int<0, max>
     */
    public ?int $estimate;

    public float $relativeError;

    /**
     * @param null|int $atMost a GUARANTEED upper bound; null when nothing is guaranteed
     * @param null|int $estimate a best guess that may be wrong in either direction
     * @param float $relativeError rough 1-sigma relative error of $estimate; meaningless without one
     */
    public function __construct(
        ?int $atMost = null,
        ?int $estimate = null,
        float $relativeError = self::DEFAULT_RELATIVE_ERROR,
    ) {
        if ($atMost !== null && $atMost < 0) {
            throw new InvalidArgumentException('Cardinality upper bound must not be negative, given: ' . $atMost);
        }

        if ($estimate !== null && $estimate < 0) {
            throw new InvalidArgumentException('Cardinality estimate must not be negative, given: ' . $estimate);
        }

        if ($relativeError < 0.0) {
            throw new InvalidArgumentException('Relative error must not be negative, given: ' . $relativeError);
        }

        $this->atMost = $atMost;
        $this->estimate = $estimate;
        $this->relativeError = $relativeError;
    }

    public static function unknown(): self
    {
        return new self();
    }

    public static function exact(int $rows): self
    {
        return new self($rows, $rows, 0.0);
    }

    public static function atMost(int $rows): self
    {
        return new self(atMost: $rows);
    }

    public static function approximately(int $rows, float $relativeError = self::DEFAULT_RELATIVE_ERROR): self
    {
        return new self(estimate: $rows, relativeError: $relativeError);
    }

    /**
     * The estimate when it is known to at least $maxRelativeError, null when it is not.
     * A caller that needs a guarantee reads $atMost instead.
     *
     * @return null|int<0, max>
     */
    public function confident(float $maxRelativeError): ?int
    {
        return $this->estimate !== null && $this->relativeError <= $maxRelativeError ? $this->estimate : null;
    }

    public function merge(self $other): self
    {
        return new self(
            atMost: $this->atMost !== null && $other->atMost !== null ? $this->atMost + $other->atMost : null,
            estimate: $this->estimate !== null && $other->estimate !== null ? $this->estimate + $other->estimate : null,
            relativeError: max($this->relativeError, $other->relativeError),
        );
    }
}
