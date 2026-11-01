<?php

declare(strict_types=1);

namespace Flow\ETL\Operators;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Exception\DataDependentSchemaException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

/**
 * Unlike a Transformer, receives the whole upstream generator, not one batch.
 */
interface Processor extends Describable
{
    /**
     * @throws DataDependentSchemaException
     *
     * @return BoundStep<Processor>
     */
    public function bind(Schema $input): BoundStep;

    /**
     * True when process() needs every input row before it emits one: the processor ends its pipeline.
     */
    public function breaks(): bool;

    /**
     * Process a stream of Rows and return a new stream.
     *
     * @param \Generator<Rows> $rows
     *
     * @return \Generator<Rows>
     */
    public function process(Generator $rows, Backend $backend): Generator;
}
