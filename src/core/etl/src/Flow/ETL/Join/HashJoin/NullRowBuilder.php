<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

final readonly class NullRowBuilder
{
    private Rows $rows;

    public function __construct(Schema $schema, Backend $backend)
    {
        $nullable = $schema->makeNullable();
        $columns = [];

        foreach ($nullable->definitions() as $name => $definition) {
            $columns[$name] = $backend->constant($definition, null, 1);
        }

        $this->rows = Rows::fromColumns($nullable, $columns, 1);
    }

    public function rows(): Rows
    {
        return $this->rows;
    }
}
