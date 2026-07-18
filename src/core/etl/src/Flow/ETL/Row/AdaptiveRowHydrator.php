<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Rows;
use Flow\ETL\Schema;

final class AdaptiveRowHydrator implements Hydrator
{
    private readonly Hydrator $delegate;

    public function __construct()
    {
        $this->delegate = NativeRowHydrator::isSupported() ? new NativeRowHydrator() : new PhpRowHydrator();
    }

    public function cast(array $batch, ?Schema $schema = null): Rows
    {
        return $this->delegate->cast($batch, $schema);
    }

    public function dehydrate(Rows $rows): array
    {
        return $this->delegate->dehydrate($rows);
    }

    public function hydrate(array $batch, ?Schema $schema = null): Rows
    {
        return $this->delegate->hydrate($batch, $schema);
    }
}
