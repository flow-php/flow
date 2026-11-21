<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\analyze;

final class AnalyzeTest extends FlowTestCase
{
    public function test_the_dataset_hash_is_collected_only_when_asked(): void
    {
        $analyze = analyze();

        static::assertFalse($analyze->collectDatasetHash());
        static::assertSame($analyze, $analyze->withDatasetHash());
        static::assertTrue($analyze->collectDatasetHash());
        static::assertFalse($analyze->collectSchema());
    }
}
