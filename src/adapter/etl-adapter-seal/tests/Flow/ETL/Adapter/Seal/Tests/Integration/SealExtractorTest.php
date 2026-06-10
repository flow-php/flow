<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration;

use CmsIg\Seal\Search\Condition\Condition;
use CmsIg\Seal\Search\SearchBuilder;
use Flow\ETL\Adapter\Seal\Tests\SealTestCase;
use Flow\ETL\Extractor\Signal;

use function Flow\ETL\Adapter\Seal\from_seal;
use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SealExtractorTest extends SealTestCase
{
    public function test_extracting_all_documents(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), str_schema('name'), int_schema('age')),
            'users',
            'id',
        ));

        $engine->bulk(
            'users',
            [
                ['id' => '1', 'name' => 'User 1', 'age' => 21],
                ['id' => '2', 'name' => 'User 2', 'age' => 22],
            ],
            [],
        );

        static::assertExtractedRowsCount(2, from_seal($engine, 'users'));
    }

    public function test_extracting_from_an_empty_index_yields_no_rows(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), str_schema('name'), int_schema('age')),
            'users',
            'id',
        ));

        static::assertExtractedRowsCount(0, from_seal($engine, 'users'));
    }

    public function test_filtering_documents_with_a_search_builder(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), str_schema('name'), int_schema('age')),
            'users',
            'id',
        ));

        $engine->bulk(
            'users',
            [
                ['id' => '1', 'name' => 'User 1', 'age' => 21],
                ['id' => '2', 'name' => 'User 2', 'age' => 22],
                ['id' => '3', 'name' => 'User 3', 'age' => 23],
            ],
            [],
        );

        $extractor = from_seal($engine, 'users')->withSearchBuilder(static function (SearchBuilder $builder): void {
            $builder->addFilter(Condition::equal('age', 21));
        });

        static::assertExtractedRowsCount(1, $extractor);
    }

    public function test_paginated_extraction_yields_multiple_batches(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), str_schema('name'), int_schema('age')),
            'users',
            'id',
        ));

        $documents = [];

        for ($i = 1; $i <= 5; $i++) {
            $documents[] = ['id' => (string) $i, 'name' => 'User ' . $i, 'age' => 20 + $i];
        }

        $engine->bulk('users', $documents, []);

        static::assertExtractedBatchesCount(3, from_seal($engine, 'users')->withPageSize(2));
        static::assertExtractedRowsCount(5, from_seal($engine, 'users')->withPageSize(2));
    }

    public function test_stop_signal_halts_extraction(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), str_schema('name'), int_schema('age')),
            'users',
            'id',
        ));

        $documents = [];

        for ($i = 1; $i <= 5; $i++) {
            $documents[] = ['id' => (string) $i, 'name' => 'User ' . $i, 'age' => 20 + $i];
        }

        $engine->bulk('users', $documents, []);

        $generator = from_seal($engine, 'users')->withPageSize(2)->extract(flow_context());

        static::assertCount(2, $generator->current());

        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
    }
}
