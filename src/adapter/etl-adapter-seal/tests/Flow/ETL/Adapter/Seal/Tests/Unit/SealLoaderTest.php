<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Unit;

use Flow\ETL\Adapter\Seal\Tests\SealTestCase;

use function Flow\ETL\Adapter\Seal\to_seal;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\rows;

final class SealLoaderTest extends SealTestCase
{
    public function test_loading_empty_rows_writes_no_documents(): void
    {
        $engine = $this->sealContext()->engine();

        to_seal($engine, self::INDEX_NAME)->load(rows(), flow_context());

        static::assertSame(0, $engine->countDocuments(self::INDEX_NAME));
    }

    public function test_with_bulk_size_returns_the_same_loader_instance(): void
    {
        $loader = to_seal($this->sealContext()->engine(), self::INDEX_NAME);

        static::assertSame($loader, $loader->withBulkSize(50));
    }
}
