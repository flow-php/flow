<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Execution\Run;
use Flow\ETL\Plan;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final readonly class FrameOutput
{
    public function __construct(
        private Plan $plan,
        private Run $run,
    ) {}

    /**
     * @return Generator<int, Rows>
     */
    public function get(): Generator
    {
        yield from $this->run->execute($this->plan);
    }

    /**
     * The merge loop of DataFrame::fetch() without the telemetry and without a limit - the plan already
     * carries it. The empty-result fallback is the value CrossJoinRowsTransformer hands Rows::joinCross()
     * when the right side is empty.
     */
    public function fetch(): Rows
    {
        $rows = null;

        foreach ($this->get() as $nextRows) {
            $rows = $rows === null ? $nextRows : $rows->merge($nextRows);
        }

        return $rows ?? new Rows($this->plan instanceof Described ? $this->plan->schema : new Schema());
    }

    /**
     * @throws SchemaNotDerivableException
     */
    public function schema(): Schema
    {
        return $this->plan instanceof Described ? $this->plan->schema : throw $this->plan->why->toException();
    }
}
