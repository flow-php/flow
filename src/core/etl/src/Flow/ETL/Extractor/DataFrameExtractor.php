<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Plan;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final class DataFrameExtractor implements RewindableExtractor
{
    private ?Schema $schema = null;

    private readonly Plan $plan;

    public function __construct(DataFrame $dataFrame)
    {
        $this->plan = $dataFrame->explain();
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
            $logical = $logical->withCursor(new Limit($logical->cursor(), $limit));
        }

        foreach ($config->executor()->execute($config->planner()->plan($logical, $this->plan->context)) as $rows) {
            if ($this->schema !== null) {
                $rows = array_to_rows($rows->toArray(), $this->schema, $context->hydrator());
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
}
