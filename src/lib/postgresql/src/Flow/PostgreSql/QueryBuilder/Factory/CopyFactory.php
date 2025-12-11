<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Factory;

use Flow\PostgreSql\QueryBuilder\Copy\{CopyFromBuilder, CopyFromSourceStep, CopyToBuilder, CopyToDestinationStep};
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

final readonly class CopyFactory
{
    public function from(string $table) : CopyFromSourceStep
    {
        return CopyFromBuilder::create()->table($table);
    }

    public function to(string $table) : CopyToDestinationStep
    {
        return CopyToBuilder::create()->table($table);
    }

    public function toQuery(SelectFinalStep $query) : CopyToDestinationStep
    {
        return CopyToBuilder::create()->query($query);
    }
}
