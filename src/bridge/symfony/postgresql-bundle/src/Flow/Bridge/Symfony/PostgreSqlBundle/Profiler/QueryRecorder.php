<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Profiler;

use function array_shift;
use function count;

final class QueryRecorder
{
    /**
     * @var list<RecordedQuery>
     */
    private array $queries = [];

    private int $failed = 0;

    private int $recorded = 0;

    private float $totalDurationMs = 0.0;

    public function __construct(
        private readonly QueryRecorderOptions $options = new QueryRecorderOptions(),
    ) {}

    public function add(RecordedQuery $query): void
    {
        $this->recorded++;
        $this->totalDurationMs += $query->durationMs;

        if ($query->failed) {
            $this->failed++;
        }

        $retainsParameters = $this->options->retainsParameters($query->parameters);
        $retainsStatement = $this->options->retainsStatement($query->sql);

        $this->queries[] = $retainsParameters && $retainsStatement
            ? $query
            : new RecordedQuery(
                $this->options->truncateStatement($query->sql),
                $retainsParameters ? $query->parameters : [],
                $query->durationMs,
                $query->rowCount,
                $query->failed,
                $query->error,
                $query->connection,
                $query->caller,
                !$retainsParameters,
                !$retainsStatement,
            );

        if (count($this->queries) > $this->options->maxQueries) {
            array_shift($this->queries);
        }
    }

    public function failedCount(): int
    {
        return $this->failed;
    }

    /**
     * @return list<RecordedQuery>
     */
    public function queries(): array
    {
        return $this->queries;
    }

    /**
     * Total queries passed to add(), including entries already evicted.
     */
    public function recordedCount(): int
    {
        return $this->recorded;
    }

    public function reset(): void
    {
        $this->queries = [];
        $this->failed = 0;
        $this->recorded = 0;
        $this->totalDurationMs = 0.0;
    }

    public function retainedCount(): int
    {
        return count($this->queries);
    }

    public function totalDurationMs(): float
    {
        return $this->totalDurationMs;
    }
}
