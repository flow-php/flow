<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\UndescribableRowLessExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class DataFrameExtractorTest extends FlowTestCase
{
    public function test_a_build_error_in_the_wrapped_frame_is_not_a_schema_refusal(): void
    {
        $extractor = from_data_frame(
            df()->read(from_rows(rows(schema(int_schema('id')), row(['id' => 1]))))->select('nope'),
        );

        try {
            $extractor->schema();

            static::fail('Expected the wrapped frame\'s build error to reach the caller.');
        } catch (InvalidArgumentException $e) {
            static::assertInstanceOf(SchemaDefinitionNotFoundException::class, $e);
            static::assertNotInstanceOf(SchemaNotDerivableException::class, $e);
        }
    }

    public function test_a_declared_schema_wins_over_the_wrapped_frame(): void
    {
        $extractor = from_data_frame(df()->read(from_rows(rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
        ))))
            ->withSchema(schema(str_schema('id')));

        $batches = iterator_to_array($extractor->extract(flow_context(config())));

        static::assertEquals(schema(str_schema('id')), $extractor->schema());
        static::assertEquals(schema(str_schema('id')), $batches[0]->schema());
        static::assertSame([['id' => '1'], ['id' => '2']], $batches[0]->toArray());
    }

    public function test_a_refusal_from_the_wrapped_frame_is_re_raised_unchanged(): void
    {
        $extractor = from_data_frame(df()->read(new UndescribableRowLessExtractor()));

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage(
            UndescribableRowLessExtractor::class
            . ' cannot describe what it will produce before producing it. '
            . 'Declare the schema with ->withSchema(), or read from a source that describes itself.',
        );

        $extractor->schema();
    }

    public function test_a_step_added_to_the_wrapped_frame_after_it_was_described_changes_the_answer(): void
    {
        $extractor = from_data_frame($inner = df()->read(from_rows(rows(schema(int_schema('id')), row(['id' => 1])))));

        static::assertEquals(schema(int_schema('id')), $extractor->schema());

        $inner->withEntry('doubled', ref('id')->multiply(lit(2)));

        static::assertEquals(schema(int_schema('id'), int_schema('doubled')), $extractor->schema());
    }

    public function test_extracting_from_another_data_frame(): void
    {
        $extractor = from_data_frame(df()->read(from_rows(
            rows(schema(str_schema('value')), row(['value' => 'test']), row(['value' => 'test'])),
            rows(schema(str_schema('value')), row(['value' => 'test']), row(['value' => 'test'])),
        )));

        self::assertExtractedRowsEquals(
            rows(
                schema(str_schema('value')),
                row(['value' => 'test']),
                row(['value' => 'test']),
                row(['value' => 'test']),
                row(['value' => 'test']),
            ),
            $extractor,
        );
    }

    public function test_it_describes_the_wrapped_frame_without_reading_it(): void
    {
        $counting = new CountingExtractor(schema(int_schema('id'), str_schema('name')));

        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            from_data_frame(df()->read($counting))->schema(),
        );
        static::assertSame(0, $counting->extractCalls);
    }
}
