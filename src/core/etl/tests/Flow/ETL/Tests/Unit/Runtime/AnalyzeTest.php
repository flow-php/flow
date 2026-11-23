<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime;

use Flow\ETL\Runtime\Executor\PartitionedOutputHasher;
use Flow\ETL\Runtime\Executor\WholeOutputHasher;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\MultisetHashAlgorithm;

use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class AnalyzeTest extends FlowTestCase
{
    public function test_the_dataset_hash_is_collected_only_when_asked(): void
    {
        $analyze = analyze();

        static::assertNull($analyze->newOutputHasher());
        static::assertSame($analyze, $analyze->withDatasetHash());
        static::assertEquals(new WholeOutputHasher(MultisetHashAlgorithm::xxh128), $analyze->newOutputHasher());
        static::assertFalse($analyze->collectSchema());
    }

    public function test_named_columns_give_a_partitioned_hasher(): void
    {
        static::assertEquals(
            new PartitionedOutputHasher(refs(ref('date'), ref('country')), MultisetHashAlgorithm::xxh128),
            analyze()->withDatasetHash([ref('date'), ref('country')])->newOutputHasher(),
        );
    }

    public function test_the_algorithm_reaches_the_hasher(): void
    {
        static::assertEquals(
            new WholeOutputHasher(MultisetHashAlgorithm::sha256),
            analyze()->withDatasetHash(algorithm: MultisetHashAlgorithm::sha256)->newOutputHasher(),
        );
        static::assertEquals(
            new PartitionedOutputHasher(refs(ref('date')), MultisetHashAlgorithm::md5),
            analyze()->withDatasetHash([ref('date')], MultisetHashAlgorithm::md5)->newOutputHasher(),
        );
    }

    public function test_each_call_builds_a_new_hasher(): void
    {
        $analyze = analyze()->withDatasetHash();

        static::assertNotSame($analyze->newOutputHasher(), $analyze->newOutputHasher());
    }
}
