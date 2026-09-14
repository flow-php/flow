<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Execution\Run;
use Flow\ETL\FlowContext;
use Flow\ETL\Plan\Described;
use Flow\ETL\Plan\Snapshot;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final class DataFrameExtractor implements NestedPlan
{
    private readonly Snapshot $snapshot;

    private ?Schema $schema = null;

    private readonly Run $run;

    public function __construct(DataFrame $dataFrame)
    {
        $this->snapshot = $dataFrame->snapshot();
        $this->run = Run::in($this->snapshot->context->config);
    }

    public function snapshot(): Snapshot
    {
        return $this->snapshot;
    }

    public function declaredSchema(): ?Schema
    {
        return $this->schema;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ($this->run->of($this->snapshot->plan, $this->snapshot->context) as $rows) {
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

        $plan = $this->run->plan($this->snapshot->plan, $this->snapshot->context);

        return $plan instanceof Described ? $plan->schema : throw $plan->why->toException();
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
