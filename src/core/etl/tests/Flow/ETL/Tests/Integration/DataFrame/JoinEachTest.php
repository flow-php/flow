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
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class JoinEachTest extends FlowTestCase
{
    public function test_join_each(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->read(from_rows(rows(
                row(int_entry('id', 1), str_entry('country', 'PL')),
                row(int_entry('id', 2), str_entry('country', 'PL')),
                row(int_entry('id', 3), str_entry('country', 'PL')),
                row(int_entry('id', 4), str_entry('country', 'PL')),
                row(int_entry('id', 5), str_entry('country', 'US')),
                row(int_entry('id', 6), str_entry('country', 'US')),
                row(int_entry('id', 7), str_entry('country', 'US')),
                row(int_entry('id', 9), str_entry('country', 'US')),
            )))
            ->batchSize(4)
            ->joinEach(
                new class implements DataFrameFactory {
                    public function from(Rows $rows): DataFrame
                    {
                        return data_frame()->process(rows(
                            row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                            row(str_entry('code', 'US'), str_entry('name', 'United States')),
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
                row(int_entry('id', 1), str_entry('country_code', 'PL')),
                row(int_entry('id', 2), str_entry('country_code', 'PL')),
                row(int_entry('id', 3), str_entry('country_code', 'PL')),
                row(int_entry('id', 4), str_entry('country_code', 'PL')),
                row(int_entry('id', 5), str_entry('country_code', 'US')),
                row(int_entry('id', 6), str_entry('country_code', 'US')),
                row(int_entry('id', 7), str_entry('country_code', 'US')),
                row(int_entry('id', 9), str_entry('country_code', 'US')),
            )))
            ->batchSize(4)
            ->joinEach(
                new class implements DataFrameFactory {
                    public function from(Rows $rows): DataFrame
                    {
                        return data_frame()->process(rows(
                            row(str_entry('country_code', 'PL'), str_entry('name', 'Poland')),
                            row(str_entry('country_code', 'US'), str_entry('name', 'United States')),
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
