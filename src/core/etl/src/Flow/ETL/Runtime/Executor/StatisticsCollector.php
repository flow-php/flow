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
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

use function gc_collect_cycles;
use function implode;
use function sprintf;

/**
 * @template T of Analyze|bool|null
 */
final class StatisticsCollector
{
    private readonly ?Analyze $analyze;

    private ?Columns $columnStatistics = null;

    private readonly RunCounts $counts;

    private ?Schema $schema = null;

    private readonly DateTimeImmutable $startedAt;

    private readonly HighResolutionTime $startTime;

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
        }

        $this->counts = new RunCounts();

        if ($this->readsColumns() && $this->context->config->launcher() !== null) {
            $this->context
                ->telemetry()
                ->logger()
                ->warn(sprintf('analyze()->%s sends every row to the parent process, so the writes do not run in parallel. '
                . 'Drop it to keep parallel writes.', implode('->', [
                    ...($this->schema === null ? [] : ['withSchema()']),
                    ...($this->columnStatistics === null ? [] : ['withColumnStatistics()']),
                ])));
        }

        gc_collect_cycles();
        $this->startedAt = $this->context->config->clock()->now();
        $this->startTime = HighResolutionTime::now();
    }

    public function capture(Rows $rows): void
    {
        $this->context->telemetry()->dataFrameBatchProcessed($rows);

        if ($this->analyze === null) {
            return;
        }

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
     * Handed to the executor under analyze, so the pipelines that read a source count its rows and a launcher its
     * workers' peaks.
     */
    public function counts(): ?RunCounts
    {
        return $this->analyze === null ? null : $this->counts;
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
                $this->context->telemetry()->totalRows(),
                new ExecutionTime($this->startedAt, $endedAt, $this->startTime->diff($endTime)),
                $this->context->telemetry()->memory(),
                $this->columnStatistics,
                $this->counts->processes(Unit::processPeak()),
            ),
            $this->analyze->collectSourceStatistics() ? $this->counts->statistics() : null,
        );
    }
}
