<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Join\Expression;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinEachTest extends FlowTestCase
{
    public function test_join_each(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country')),
                row(['id' => 1, 'country' => 'PL']),
                row(['id' => 2, 'country' => 'PL']),
                row(['id' => 3, 'country' => 'PL']),
                row(['id' => 4, 'country' => 'PL']),
                row(['id' => 5, 'country' => 'US']),
                row(['id' => 6, 'country' => 'US']),
                row(['id' => 7, 'country' => 'US']),
                row(['id' => 9, 'country' => 'US']),
            )))
            ->batchSize(4)
            ->joinEach(
                new class implements DataFrameFactory {
                    public function from(Rows $rows): DataFrame
                    {
                        return data_frame()->process(rows(
                            schema(str_schema('code'), str_schema('name')),
                            row(['code' => 'PL', 'name' => 'Poland']),
                            row(['code' => 'US', 'name' => 'United States']),
                        ));
                    }
                },
                Expression::on(['country' => 'code'], 'joined_'),
            )
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 3, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 4, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 5, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 6, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 7, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 9, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
            ],
            $rows->toArray(),
        );
    }

    public function test_join_each_without_prefix(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('country_code')),
                row(['id' => 1, 'country_code' => 'PL']),
                row(['id' => 2, 'country_code' => 'PL']),
                row(['id' => 3, 'country_code' => 'PL']),
                row(['id' => 4, 'country_code' => 'PL']),
                row(['id' => 5, 'country_code' => 'US']),
                row(['id' => 6, 'country_code' => 'US']),
                row(['id' => 7, 'country_code' => 'US']),
                row(['id' => 9, 'country_code' => 'US']),
            )))
            ->batchSize(4)
            ->joinEach(
                new class implements DataFrameFactory {
                    public function from(Rows $rows): DataFrame
                    {
                        return data_frame()->process(rows(
                            schema(str_schema('country_code'), str_schema('name')),
                            row(['country_code' => 'PL', 'name' => 'Poland']),
                            row(['country_code' => 'US', 'name' => 'United States']),
                        ));
                    }
                },
                join_on(['country_code' => 'country_code']),
            )
            ->write($loader)
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 3, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 4, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 5, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 6, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 7, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 9, 'country_code' => 'US', 'name' => 'United States'],
            ],
            $rows->toArray(),
        );
    }
}
