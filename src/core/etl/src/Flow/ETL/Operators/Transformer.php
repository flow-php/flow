<?php

declare(strict_types=1);

namespace Flow\ETL\Operators;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Exception\DataDependentSchemaException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

interface Transformer extends Describable
{
    /**
     * @throws DataDependentSchemaException
     *
     * @return BoundStep<Transformer>
     */
    public function bind(Schema $input): BoundStep;

    public function transform(Rows $rows, Backend $backend): Rows;
}
