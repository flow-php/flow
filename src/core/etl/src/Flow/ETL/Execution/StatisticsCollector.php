<?php

declare(strict_types=1);

namespace Flow\ETL\Execution;

use Flow\ETL\{Analyze, FlowContext, Rows, Schema};
use Flow\ETL\Dataset\Memory\Consumption;
use Flow\ETL\Dataset\{Report, Statistics};
use Flow\ETL\Dataset\Statistics\{Columns, ExecutionTime, HighResolutionTime};

/**
 * @template T of Analyze|bool|null
 */
final class StatisticsCollector
{
    private readonly ?Analyze $analyze;

    private ?Columns $columnStatistics = null;

    private readonly Consumption $memory;

    private ?Schema $schema = null;

    private readonly \DateTimeImmutable $startedAt;

    private readonly HighResolutionTime $startTime;

    private int $totalRows = 0;

    /**
     * @param T $analyze
     */
    public function __construct(
        Analyze|bool|null $analyze,
        private readonly FlowContext $context,
    ) {
        $this->analyze = match (true) {
            $analyze === true => new Analyze(),
            $analyze === false || $analyze === null => null,
            default => $analyze,
        };

        if ($this->analyze !== null) {
            $this->columnStatistics = $this->analyze->collectColumnStatistics() ? new Columns() : null;
            $this->schema = $this->analyze->collectSchema() ? new Schema() : null;
        }

        \gc_collect_cycles();
        $this->memory = new Consumption();
        $this->startedAt = $this->context->config->clock()->now();
        $this->startTime = HighResolutionTime::now();
    }

    public function capture(Rows $rows) : void
    {
        $this->totalRows += $rows->count();

        $this->context->telemetry()->dataFrameBatchProcessed($rows, $this->context);

        if ($this->analyze === null) {
            return;
        }

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

    public function end(?\Throwable $exception = null) : void
    {
        if ($exception !== null) {
            $this->context->telemetry()->logger()->error('Data frame processing failed', ['exception' => $exception->getMessage()]);
        }

        $this->context->telemetry()->dataFrameCompleted($this->context);
    }

    /**
     * @return (T is Analyze|true ? Report : null)
     */
    public function report() : ?Report
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
                $this->columnStatistics
            )
        );
    }
}
