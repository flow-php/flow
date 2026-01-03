<?php

declare(strict_types=1);

namespace Flow\ETL\Execution;

use Flow\Clock\SystemClock;
use Flow\ETL\{Analyze, Rows, Schema};
use Flow\ETL\Dataset\Memory\Consumption;
use Flow\ETL\Dataset\{Report, Statistics};
use Flow\ETL\Dataset\Statistics\{Columns, ExecutionTime, HighResolutionTime};
use Psr\Clock\ClockInterface;

/**
 * @template T of Analyze|bool|null
 */
final class ReportCollector
{
    private readonly ?Analyze $analyze;

    private readonly ClockInterface $clock;

    private ?Columns $columnStatistics = null;

    private ?Consumption $memory = null;

    private ?Schema $schema = null;

    private ?\DateTimeImmutable $startedAt = null;

    private ?HighResolutionTime $startTime = null;

    private int $totalRows = 0;

    /**
     * @param T $analyze
     */
    public function __construct(
        Analyze|bool|null $analyze,
        ?ClockInterface $clock = null,
    ) {
        $this->clock = $clock ?? SystemClock::system();

        $this->analyze = match (true) {
            $analyze === true => new Analyze(),
            $analyze === false || $analyze === null => null,
            default => $analyze,
        };

        if ($this->analyze !== null) {
            \gc_collect_cycles();
            $this->memory = new Consumption();
            $this->startedAt = $this->clock->now();
            $this->startTime = HighResolutionTime::now();
            $this->columnStatistics = $this->analyze->collectColumnStatistics() ? new Columns() : null;
            $this->schema = $this->analyze->collectSchema() ? new Schema() : null;
        }
    }

    public function capture(Rows $rows) : void
    {
        if ($this->analyze === null || $this->memory === null) {
            return;
        }

        $this->totalRows += $rows->count();
        $this->memory->capture();

        if ($this->schema !== null) {
            $this->schema = $this->schema->merge($rows->schema());
        }

        if ($this->columnStatistics !== null) {
            foreach ($rows->all() as $row) {
                foreach ($row->entries()->all() as $entry) {
                    $this->columnStatistics->add($entry);
                }
            }
        }
    }

    /**
     * @return (T is Analyze|true ? Report : null)
     */
    public function report() : ?Report
    {
        if ($this->analyze === null || $this->memory === null || $this->startedAt === null || $this->startTime === null) {
            return null;
        }

        $endedAt = $this->clock->now();
        $endTime = HighResolutionTime::now();

        return new Report(
            $this->schema,
            new Statistics(
                $this->totalRows,
                new ExecutionTime($this->startedAt, $endedAt, $this->startTime->diff($endTime)),
                $this->memory,
                $this->columnStatistics
            )
        );
    }
}
