<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\Extractor\PhpJsonOpenSource;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class PhpJsonOpenSourceTest extends FlowTestCase
{
    public function test_batches_are_sized_and_the_tail_is_shorter(): void
    {
        $batches = iterator_to_array(
            (new PhpJsonOpenSource(
                JsonFixtureContext::reader(),
                JsonFixtureContext::source('five_rows.json'),
            ))->batches(schema(int_schema('id')), 2, new PhpBackend()),
            false,
        );

        static::assertSame([2, 2, 1], array_map(static fn(Rows $rows): int => $rows->count(), $batches));
        static::assertSame([['id' => 5]], $batches[2]->toArray());
    }

    public function test_a_refusal_carries_its_row_in_the_whole_source(): void
    {
        $batches = (new PhpJsonOpenSource(
            JsonFixtureContext::reader(JsonFraming::Array),
            JsonFixtureContext::source('misfit.json'),
        ))->batches(schema(int_schema('id')), 2, new PhpBackend());

        static::assertSame([['id' => 1], ['id' => 2]], $batches->current()->toArray());

        try {
            $batches->next();
            static::fail('the second batch holds "n/a"');
        } catch (SchemaMismatchException $e) {
            static::assertSame(3, $e->rowIndex);
        }
    }

    #[TestWith([JsonFraming::Array, 'five_rows.json'])]
    #[TestWith([JsonFraming::Lines, 'five_rows.jsonl'])]
    public function test_batches_close_the_stream_when_abandoned(JsonFraming $format, string $fixture): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $batches = (new PhpJsonOpenSource(
            JsonFixtureContext::reader($format, $counting),
            JsonFixtureContext::source($fixture),
        ))->batches(schema(int_schema('id')), 2, new PhpBackend());

        static::assertSame(2, $batches->current()->count());
        unset($batches);

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(1, $counting->closedStreams());
    }

    #[TestWith([JsonFraming::Array, 'empty_array.json'])]
    #[TestWith([JsonFraming::Array, 'array_of_empty_object.json'])]
    #[TestWith([JsonFraming::Array, 'empty.json'])]
    #[TestWith([JsonFraming::Lines, 'empty.jsonl'])]
    #[TestWith([JsonFraming::Lines, 'only_blank_lines.jsonl'])]
    public function test_batches_never_yield_an_empty_batch(JsonFraming $format, string $fixture): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        static::assertSame(
            [],
            iterator_to_array(
                (new PhpJsonOpenSource(
                    JsonFixtureContext::reader($format, $counting),
                    JsonFixtureContext::source($fixture),
                ))->batches(schema(int_schema('id')), 2, new PhpBackend()),
                false,
            ),
        );
        static::assertSame(1, $counting->closedStreams());
    }
}
