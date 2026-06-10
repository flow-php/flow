<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Unit;

use CmsIg\Seal\Search\SearchBuilder;
use Flow\ETL\Adapter\Seal\Tests\SealTestCase;

use function Flow\ETL\Adapter\Seal\from_seal;
use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SealExtractorTest extends SealTestCase
{
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
