<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Pipeline\BoundStep;

interface Transformer
{
    /**
     * @throws DataDependentSchemaException
     */
    public function bind(Schema $input): BoundStep;

    /**
     * @throws LimitReachedException
     */
    public function transform(Rows $rows, FlowContext $context): Rows;
}
