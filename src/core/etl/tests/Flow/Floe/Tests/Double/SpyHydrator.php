<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Double;

use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

final class SpyHydrator implements Hydrator
{
    public int $dehydrateCalls = 0;

    public function cast(array $batch, ?Schema $schema = null): Rows
    {
        return (new AdaptiveRowHydrator())->cast($batch, $schema);
    }

    public function dehydrate(Rows $rows): array
    {
        $this->dehydrateCalls++;

        return (new AdaptiveRowHydrator())->dehydrate($rows);
    }

    public function hydrate(array $batch, ?Schema $schema = null): Rows
    {
        return (new AdaptiveRowHydrator())->hydrate($batch, $schema);
    }
}
