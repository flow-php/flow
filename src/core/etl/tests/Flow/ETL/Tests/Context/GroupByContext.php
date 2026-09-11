<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\GroupBySteps;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Generator;

use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class GroupByContext
{
    /**
     * Runs input batches through the GroupBySteps chain the way Pipeline does.
     *
     * @return list<Rows>
     */
    public static function aggregate(GroupBy $groupBy, FlowContext $context, Rows ...$batches): array
    {
        $output = self::batches(...$batches);

        foreach (GroupBySteps::of($groupBy, $context->config) as $step) {
            $output = $step instanceof Processor
                ? $step->process($output, $context)
                : self::transformed($step, $output, $context);
        }

        return iterator_to_array($output, preserve_keys: false);
    }

    /**
     * All aggregated batches merged into one Rows, for equality assertions.
     */
    public static function aggregated(GroupBy $groupBy, FlowContext $context, Rows ...$batches): Rows
    {
        $result = null;

        foreach (self::aggregate($groupBy, $context, ...$batches) as $batch) {
            $result = $result === null ? $batch : $result->merge($batch);
        }

        return $result ?? rows(schema());
    }

    /**
     * @return Generator<Rows>
     */
    public static function batches(Rows ...$batches): Generator
    {
        yield from $batches;
    }

    /**
     * Spills $batches into $buckets the way BucketingProcessor does, returning one metadata Rows per
     * bucket - the input GroupByAggregationProcessor::process() consumes.
     *
     * @param list<Reference> $refs
     *
     * @return list<Rows>
     */
    public static function bucketMetadata(Buckets $buckets, array $refs, Rows ...$batches): array
    {
        $strategy = new HashBucketing($refs, 4, new NativeHasher(), new NativePHPRandomValueGenerator(), 'group-by');

        $metadata = [];

        foreach ($strategy->bucketize(self::batches(...$batches), $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
            $metadata[] = rows(Bucket::schema(), $bucket->toRow());
        }

        return $metadata;
    }

    /**
     * @param Generator<Rows> $upstream
     *
     * @return Generator<Rows>
     */
    public static function transformed(Transformer $step, Generator $upstream, FlowContext $context): Generator
    {
        foreach ($upstream as $batch) {
            yield $step->transform($batch, $context);
        }
    }
}
