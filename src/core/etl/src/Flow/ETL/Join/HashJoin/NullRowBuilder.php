<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

final readonly class NullRowBuilder
{
    private Row $row;

    public function __construct(Schema $schema)
    {
        $nullable = $schema->makeNullable();
        $backend = new PhpBackend();
        $columns = [];

        foreach ($nullable->definitions() as $name => $definition) {
            $columns[$name] = $backend->constant($definition, null, 1);
        }

        $this->row = Rows::fromColumns($nullable, $columns, 1)->row(0);
    }

    public function row(): Row
    {
        return $this->row;
    }
}
