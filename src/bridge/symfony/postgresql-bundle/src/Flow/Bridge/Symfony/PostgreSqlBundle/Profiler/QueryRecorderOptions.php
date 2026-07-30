<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Profiler;

use InvalidArgumentException;

use function count;
use function strlen;
use function substr;

final readonly class QueryRecorderOptions
{
    /**
     * @param int $maxQueries Maximum retained entries; the oldest is evicted first
     * @param bool $includeParameters Whether bound parameters are retained at all
     * @param null|int $maxRetainedParameters Entries binding more parameters than this keep none of them (null = unlimited)
     * @param null|int $maxQueryLength Retained statements longer than this are truncated (null = unlimited)
     */
    public function __construct(
        public int $maxQueries = 1000,
        public bool $includeParameters = true,
        public ?int $maxRetainedParameters = 100,
        public ?int $maxQueryLength = 1000,
    ) {
        if ($maxQueries < 1) {
            throw new InvalidArgumentException('QueryRecorderOptions::$maxQueries must be at least 1, got '
            . $maxQueries);
        }

        if ($maxRetainedParameters !== null && $maxRetainedParameters < 0) {
            throw new InvalidArgumentException('QueryRecorderOptions::$maxRetainedParameters must not be negative, got '
            . $maxRetainedParameters);
        }

        if ($maxQueryLength !== null && $maxQueryLength < 1) {
            throw new InvalidArgumentException('QueryRecorderOptions::$maxQueryLength must be at least 1, got '
            . $maxQueryLength);
        }
    }

    public function includeParameters(bool $include = true): self
    {
        return new self($this->maxQueries, $include, $this->maxRetainedParameters, $this->maxQueryLength);
    }

    public function maxQueries(int $max): self
    {
        return new self($max, $this->includeParameters, $this->maxRetainedParameters, $this->maxQueryLength);
    }

    public function maxQueryLength(?int $max): self
    {
        return new self($this->maxQueries, $this->includeParameters, $this->maxRetainedParameters, $max);
    }

    public function maxRetainedParameters(?int $max): self
    {
        return new self($this->maxQueries, $this->includeParameters, $max, $this->maxQueryLength);
    }

    /**
     * Parameters with nothing to drop count as retained, so a parameterless query is never
     * reported as truncated.
     *
     * @param list<mixed> $parameters
     */
    public function retainsParameters(array $parameters): bool
    {
        if (!$this->includeParameters) {
            return $parameters === [];
        }

        if ($this->maxRetainedParameters === null) {
            return true;
        }

        return count($parameters) <= $this->maxRetainedParameters;
    }

    public function retainsStatement(string $sql): bool
    {
        return $this->maxQueryLength === null || strlen($sql) <= $this->maxQueryLength;
    }

    public function truncateStatement(string $sql): string
    {
        return $this->retainsStatement($sql) ? $sql : substr($sql, 0, (int) $this->maxQueryLength) . '...';
    }
}
