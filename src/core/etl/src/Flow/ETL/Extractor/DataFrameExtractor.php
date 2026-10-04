<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Plan;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Trigger;
use Flow\ETL\Rows;
use Flow\ETL\Rows\ColumnRetyping;
use Flow\ETL\Schema;
use Generator;

final class DataFrameExtractor implements RewindableExtractor
{
    private ?Schema $schema = null;

    private readonly Plan $plan;

    public function __construct(DataFrame $dataFrame)
    {
        $this->plan = $dataFrame->explain(Trigger::rows);
    }

    /**
     * Runs the frame with its own configuration; a limit stops the frame's rows, so its own rules push it into its
     * source.
     *
     * @param null|positive-int $limit
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        $logical = $this->plan->logical;
        $config = $this->plan->context->config;

        if ($limit !== null) {
            $logical = Trigger::rows->plan(new Limit($logical->spine(), $limit), $logical->sinks());
        }

        foreach ($config->executor()->execute($config->planner()->plan($logical, $this->plan->context)) as $rows) {
            if ($this->schema !== null) {
                $backend = $context->backend();
                $retyping = new ColumnRetyping();
                $columns = [];

                foreach ($this->schema->definitions() as $name => $definition) {
                    $own = $rows->schema()->findDefinition($definition->entry()->name());

                    $columns[$name] = $own === null
                        ? $retyping->absent($definition, $rows->count(), $backend)
                        : $retyping->cast($rows->column($definition->entry()->name()), $own, $definition, $backend);
                }

                $rows = Rows::fromColumns($this->schema, $columns, $rows->count());
            }

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    public function isRepeatable(): bool
    {
        return (new Repeatability())->ofPlan($this->plan->logical);
    }

    /**
     * @throws SchemaNotDerivableException
     */
    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        return $this->plan
            ->context
            ->config
            ->planner()
            ->plan($this->plan->logical, $this->plan->context)
            ->schema();
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }
}
