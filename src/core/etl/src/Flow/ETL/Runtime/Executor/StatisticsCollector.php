<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use DateTimeImmutable;
use Flow\ETL\Foundation\Clock\HighResolutionTime;
use Flow\ETL\Runtime\Analyze;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Runtime\Metrics\Report;
use Flow\ETL\Runtime\Metrics\Statistics;
use Flow\ETL\Runtime\Metrics\Statistics\Columns;
use Flow\ETL\Runtime\Metrics\Statistics\ExecutionTime;
use Flow\ETL\Values\Memory\Consumption;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

use function gc_collect_cycles;

/**
 * @template T of Analyze|bool|null
 */
final class StatisticsCollector
{
    private readonly ?Analyze $analyze;

    private ?Columns $columnStatistics = null;

    private readonly Consumption $memory;

    private ?Schema $schema = null;

    private ?SourceCounts $sources = null;

    private readonly DateTimeImmutable $startedAt;

    private readonly HighResolutionTime $startTime;

    private int $totalRows = 0;

    /**
     * @param T $analyze
     */
    public function __construct(
        Analyze|bool|null $analyze,
        private readonly FlowContext $context,
    ) {
        // @mago-ignore analysis:invalid-property-assignment-value
        $this->analyze = match (true) {
            $analyze === true => new Analyze(),
            // @mago-ignore analysis:impossible-type-comparison
            $analyze === false || $analyze === null => null,
            default => $analyze,
        };

        if ($this->analyze !== null) {
            $this->columnStatistics = $this->analyze->collectColumnStatistics() ? new Columns() : null;
            $this->schema = $this->analyze->collectSchema() ? new Schema() : null;
            $this->sources = $this->analyze->collectSourceStatistics() ? new SourceCounts() : null;
        }

        gc_collect_cycles();
        $this->memory = new Consumption($this->context->backend());
        $this->startedAt = $this->context->config->clock()->now();
        $this->startTime = HighResolutionTime::now();
    }

    public function capture(Rows $rows): void
    {
        $this->totalRows += $rows->count();

        $this->context->telemetry()->dataFrameBatchProcessed($rows);

        if ($this->analyze === null) {
            return;
        }

        $this->memory->capture();

        if ($this->schema !== null) {
            $this->schema = $this->schema->merge($rows->schema());
        }

        if ($this->columnStatistics !== null && !$rows->isEmpty()) {
            foreach ($rows->schema()->definitions() as $name => $definition) {
                $this->columnStatistics->add($definition, $rows->column($name));
            }
        }
    }

    /**
     * The run reads the output's columns: a schema or column statistics are collected, so every row has to reach
     * this process.
     */
    public function readsColumns(): bool
    {
        return $this->schema !== null || $this->columnStatistics !== null;
    }

    /**
     * Handed to the planner, so the pipelines that read a source count its rows.
     */
    public function sources(): ?SourceCounts
    {
        return $this->sources;
    }

    /**
     * @return (T is Analyze|true ? Report : null)
     */
    public function report(): ?Report
    {
        if ($this->analyze === null) {
            return null;
        }

        $endedAt = $this->context->config->clock()->now();
        $endTime = HighResolutionTime::now();

        return new Report(
            $this->schema,
            new Statistics(
                $this->totalRows,
                new ExecutionTime($this->startedAt, $endedAt, $this->startTime->diff($endTime)),
                $this->memory,
                $this->columnStatistics,
            ),
            $this->sources?->statistics(),
        );
    }
}
