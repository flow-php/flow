<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Unit;

use CmsIg\Seal\Search\Condition\Condition;
use CmsIg\Seal\Search\SearchBuilder;
use Flow\ETL\Adapter\Seal\Direction;
use Flow\ETL\Adapter\Seal\Tests\SealTestCase;
use Flow\ETL\Extractor\Signal;

use function Flow\ETL\Adapter\Seal\from_seal;
use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SealExtractorTest extends SealTestCase
{
    public function test_keyset_pagination_descending_order(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), int_schema('num')),
            'items',
            'id',
        ));
        $engine->bulk(
            'items',
            [
                ['id' => '2', 'num' => 20],
                ['id' => '1', 'num' => 10],
                ['id' => '3', 'num' => 30],
            ],
            [],
        );

        $values = [];

        foreach (data_frame()
            ->read(from_seal($engine, 'items')->withKeysetPagination('num', Direction::DESC))
            ->fetch() as $row) {
            $values[] = $row->valueOf('num');
        }

        static::assertSame([30, 20, 10], $values);
    }

    public function test_keyset_pagination_extracts_all_rows_across_pages(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), int_schema('num')),
            'items',
            'id',
        ));
        $engine->bulk(
            'items',
            [
                ['id' => '1', 'num' => 10],
                ['id' => '2', 'num' => 20],
                ['id' => '3', 'num' => 30],
                ['id' => '4', 'num' => 40],
                ['id' => '5', 'num' => 50],
            ],
            [],
        );

        static::assertExtractedRowsCount(5, from_seal($engine, 'items')->withKeysetPagination('num')->withPageSize(2));
        static::assertExtractedBatchesCount(
            3,
            from_seal($engine, 'items')->withKeysetPagination('num')->withPageSize(2),
        );
    }

    public function test_keyset_pagination_preserves_ascending_order(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), int_schema('num')),
            'items',
            'id',
        ));
        $engine->bulk(
            'items',
            [
                ['id' => '2', 'num' => 20],
                ['id' => '1', 'num' => 10],
                ['id' => '3', 'num' => 30],
            ],
            [],
        );

        $values = [];

        foreach (data_frame()
            ->read(from_seal($engine, 'items')->withKeysetPagination('num')->withPageSize(1))
            ->fetch() as $row) {
            $values[] = $row->valueOf('num');
        }

        static::assertSame([10, 20, 30], $values);
    }

    public function test_keyset_pagination_respects_stop_signal(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), int_schema('num')),
            'items',
            'id',
        ));
        $engine->bulk(
            'items',
            [
                ['id' => '1', 'num' => 10],
                ['id' => '2', 'num' => 20],
                ['id' => '3', 'num' => 30],
                ['id' => '4', 'num' => 40],
                ['id' => '5', 'num' => 50],
            ],
            [],
        );

        $generator = from_seal($engine, 'items')->withKeysetPagination('num')->withPageSize(2)->extract(flow_context());

        static::assertCount(2, $generator->current());

        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
    }

    public function test_keyset_pagination_with_a_search_builder_filter(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            schema(str_schema('id'), int_schema('num')),
            'items',
            'id',
        ));
        $engine->bulk(
            'items',
            [
                ['id' => '1', 'num' => 10],
                ['id' => '2', 'num' => 20],
                ['id' => '3', 'num' => 30],
                ['id' => '4', 'num' => 40],
                ['id' => '5', 'num' => 50],
            ],
            [],
        );

        $extractor = from_seal($engine, 'items')
            ->withKeysetPagination('num')
            ->withPageSize(2)
            ->withSearchBuilder(static function (SearchBuilder $builder): void {
                $builder->addFilter(Condition::greaterThanEqual('num', 30));
            });

        static::assertExtractedRowsCount(3, $extractor);
    }

    public function test_with_keyset_pagination_returns_the_same_extractor_instance(): void
    {
        $extractor = from_seal(
            $this->sealContext()->engine(to_seal_schema(schema(str_schema('id')), 'users', 'id')),
            'users',
        );

        static::assertSame($extractor, $extractor->withKeysetPagination('id'));
    }

    public function test_with_page_size_returns_the_same_extractor_instance(): void
    {
        $extractor = from_seal(
            $this->sealContext()->engine(to_seal_schema(schema(str_schema('id')), 'users', 'id')),
            'users',
        );

        static::assertSame($extractor, $extractor->withPageSize(100));
    }

    public function test_with_search_builder_returns_the_same_extractor_instance(): void
    {
        $extractor = from_seal(
            $this->sealContext()->engine(to_seal_schema(schema(str_schema('id')), 'users', 'id')),
            'users',
        );

        static::assertSame($extractor, $extractor->withSearchBuilder(static function (SearchBuilder $builder): void {}));
    }
}
