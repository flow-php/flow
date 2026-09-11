<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;
use function usort;

final class RepartitionProcessorTest extends FlowTestCase
{
    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(str_schema('k'), int_schema('v'));
        $processor = new RepartitionProcessor(refs(ref('k')), new Buckets(new MemoryBuckets()));

        static::assertEquals($input, $processor->bind($input)->output);
    }

    /**
     * One bucket proves the grouping is the processor's own work; 64 proves a key is never split
     * across buckets, which is what makes the downstream window contiguous.
     *
     * @param int<1, max> $bucketsCount
     */
    #[TestWith([1])]
    #[TestWith([64])]
    public function test_every_row_sharing_a_key_arrives_in_one_batch(int $bucketsCount): void
    {
        $schema = schema(str_schema('k'), int_schema('v'));
        $buckets = new Buckets(new MemoryBuckets());
        $strategy = new HashBucketing(
            [ref('k')],
            $bucketsCount,
            new NativeHasher(),
            new NativePHPRandomValueGenerator(),
            'repartition',
        );

        $input = (static function () use ($schema): Generator {
            yield rows($schema, row(['k' => 'a', 'v' => 1]), row(['k' => 'b', 'v' => 2]));
            yield rows($schema, row(['k' => 'a', 'v' => 3]), row(['k' => 'b', 'v' => 4]));
        })();

        $metadata = [];

        foreach ($strategy->bucketize($input, $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
            $metadata[] = rows(Bucket::schema(), $bucket->toRow());
        }

        $groups = array_map(
            static fn(Rows $group): array => array_map(
                static fn(array $values): int => (int) $values['v'],
                $group->toArray(),
            ),
            iterator_to_array(
                (new RepartitionProcessor(refs(ref('k')), $buckets))->process(
                    (static function () use ($metadata): Generator {
                        yield from $metadata;
                    })(),
                    flow_context(),
                ),
                preserve_keys: false,
            ),
        );

        usort($groups, static fn(array $a, array $b): int => $a[0] <=> $b[0]);

        static::assertSame([[1, 3], [2, 4]], $groups);
    }

    public function test_buckets_are_cleared_when_the_stream_is_done(): void
    {
        $schema = schema(str_schema('k'), int_schema('v'));
        $buckets = new Buckets(new MemoryBuckets());
        $strategy = new HashBucketing(
            [ref('k')],
            4,
            new NativeHasher(),
            new NativePHPRandomValueGenerator(),
            'repartition',
        );

        $metadata = [];

        foreach ($strategy->bucketize(
            (static function () use ($schema): Generator {
                yield rows($schema, row(['k' => 'a', 'v' => 1]));
            })(),
            $buckets->storage(),
        ) as $bucket) {
            $buckets->add($bucket);
            $metadata[] = rows(Bucket::schema(), $bucket->toRow());
        }

        iterator_to_array(
            (new RepartitionProcessor(refs(ref('k')), $buckets))->process(
                (static function () use ($metadata): Generator {
                    yield from $metadata;
                })(),
                flow_context(),
            ),
            preserve_keys: false,
        );

        static::assertSame([], $buckets->all());
    }
}
