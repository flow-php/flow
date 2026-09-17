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

final class DataFrameExtractor implements NestedPlan
{
    private ?Schema $schema = null;

    private readonly Plan $plan;

    public function __construct(DataFrame $dataFrame)
    {
        $this->plan = $dataFrame->explain();
    }

    /**
     * @param null|positive-int $limit
     */
    public function plan(?int $limit = null): Plan
    {
        if ($limit === null) {
            return $this->plan;
        }

        $logical = $this->plan->logical;

        return Plan::of($logical->withCursor(new Limit($logical->cursor(), $limit)), $this->plan->context);
    }

    public function declaredSchema(): ?Schema
    {
        return $this->schema;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        $plan = $this->plan($limit);
        $config = $plan->context->config;

        foreach ($config->executor()->execute($config->planner()->plan($plan->logical, $plan->context)) as $rows) {
            if ($this->schema !== null) {
                $rows = array_to_rows($rows->toArray(), $this->schema, $context->hydrator());
            }

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }
        }
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
