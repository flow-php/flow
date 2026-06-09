<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration;

use CmsIg\Seal\Search\Condition\Condition;
use CmsIg\Seal\Search\SearchBuilder;
use Flow\ETL\Adapter\Seal\Tests\SealTestCase;
use Flow\ETL\Extractor\Signal;

use function Flow\ETL\Adapter\Seal\from_seal;
use function Flow\ETL\DSL\flow_context;

final class SealExtractorTest extends SealTestCase
{
    public function test_extracting_all_documents(): void
    {
        $this->seed(2);

        static::assertExtractedRowsCount(2, from_seal($this->sealContext()->engine(), self::INDEX_NAME));
    }

    public function test_extracting_from_an_empty_index_yields_no_rows(): void
    {
        static::assertExtractedRowsCount(0, from_seal($this->sealContext()->engine(), self::INDEX_NAME));
    }

    public function test_filtering_documents_with_a_search_builder(): void
    {
        $this->seed(3);

        $extractor = from_seal(
            $this->sealContext()->engine(),
            self::INDEX_NAME,
        )->withSearchBuilder(static function (SearchBuilder $builder): void {
            $builder->addFilter(Condition::equal('age', 21));
        });

        static::assertExtractedRowsCount(1, $extractor);
    }

    public function test_paginated_extraction_yields_multiple_batches(): void
    {
        $this->seed(5);

        static::assertExtractedBatchesCount(
            3,
            from_seal($this->sealContext()->engine(), self::INDEX_NAME)->withPageSize(2),
        );
        static::assertExtractedRowsCount(
            5,
            from_seal($this->sealContext()->engine(), self::INDEX_NAME)->withPageSize(2),
        );
    }

    public function test_stop_signal_halts_extraction(): void
    {
        $this->seed(5);

        $generator = from_seal($this->sealContext()->engine(), self::INDEX_NAME)
            ->withPageSize(2)
            ->extract(flow_context());

        static::assertCount(2, $generator->current());

        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
    }

    private function seed(int $count): void
    {
        $documents = [];

        for ($i = 1; $i <= $count; $i++) {
            $documents[] = ['id' => (string) $i, 'name' => 'User ' . $i, 'age' => 20 + $i];
        }

        $this->sealContext()->engine()->bulk(self::INDEX_NAME, $documents, []);
    }
}
