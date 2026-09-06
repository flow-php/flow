<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Rows;
use Flow\ETL\Schema;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustRowHydratorNative
{
    public function __construct()
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @param list<RawRowValues> $batch
     */
    public function hydrate(array $batch, Schema $schema): Rows
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return list<TypedRowValues>
     */
    public function dehydrate(Rows $rows): array
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
