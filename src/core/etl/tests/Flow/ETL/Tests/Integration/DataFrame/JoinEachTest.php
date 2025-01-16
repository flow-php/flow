<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{df, from_rows, int_entry, row, rows, str_entry};
use Flow\ETL\Join\Expression;
use Flow\ETL\{DataFrame, DataFrameFactory, Loader, Rows, Tests\FlowTestCase};

final class JoinEachTest extends FlowTestCase
{
    public function test_join_each() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))
            ->method('load');

        $rows = df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL')),
                    row(int_entry('id', 2), str_entry('country', 'PL')),
                    row(int_entry('id', 3), str_entry('country', 'PL')),
                    row(int_entry('id', 4), str_entry('country', 'PL')),
                    row(int_entry('id', 5), str_entry('country', 'US')),
                    row(int_entry('id', 6), str_entry('country', 'US')),
                    row(int_entry('id', 7), str_entry('country', 'US')),
                    row(int_entry('id', 9), str_entry('country', 'US')),
                )
            ))
            ->batchSize(4)
            ->joinEach(
                new class implements DataFrameFactory {
                    public function from(Rows $rows) : DataFrame
                    {
                        return data_frame()->process(
                            rows(
                                row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                                row(str_entry('code', 'US'), str_entry('name', 'United States')),
                            )
                        );
                    }
                },
                Expression::on(['country' => 'code']),
            )
            ->write($loader)
            ->fetch();

        self::assertEquals(
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
            $rows->toArray()
        );
    }
}
