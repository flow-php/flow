<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\Extractor\PhpJsonOpenSource;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
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

        static::assertContainsOnlyInstancesOf(Rows::class, $batches);
        static::assertSame(
            [[['id' => 1], ['id' => 2]], [['id' => 3], ['id' => 4]], [['id' => 5]]],
            array_map(static fn(CorruptedRecordException|Rows $rows): array => (
                $rows instanceof Rows ? $rows->toArray() : [$rows->getMessage()]
            ), $batches),
        );
    }

    public function test_under_a_declared_schema_a_refused_element_is_a_corrupted_record_numbered_in_the_document(): void
    {
        $batches = (new PhpJsonOpenSource(
            JsonFixtureContext::reader(JsonFraming::Array),
            JsonFixtureContext::source('misfit.json'),
        ))->batches(schema(int_schema('id')), 2, new PhpBackend());

        $rows = $batches->current();

        static::assertInstanceOf(Rows::class, $rows);
        static::assertSame([['id' => 1], ['id' => 2]], $rows->toArray());

        $batches->next();
        $corrupted = $batches->current();

        static::assertInstanceOf(CorruptedRecordException::class, $corrupted);
        static::assertSame([4, null], [$corrupted->offset, $corrupted->lineNumber]);
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

        $rows = $batches->current();

        static::assertInstanceOf(Rows::class, $rows);
        static::assertSame(2, $rows->count());
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
