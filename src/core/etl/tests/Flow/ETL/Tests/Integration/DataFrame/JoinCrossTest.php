<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{bool_entry, df, from_rows, int_entry, row, rows, str_entry};
use Flow\ETL\{Loader, Tests\FlowTestCase};

final class JoinCrossTest extends FlowTestCase
{
    public function test_cross_join() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))
            ->method('load');

        $rows = df()
            ->from(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL')),
                    row(int_entry('id', 2), str_entry('country', 'PL')),
                    row(int_entry('id', 3), str_entry('country', 'PL')),
                    row(int_entry('id', 4), str_entry('country', 'PL')),
                )
            ))
            ->batchSize(2)
            ->crossJoin(
                (data_frame())->process(
                    rows(
                        row(int_entry('num', 1), bool_entry('active', true)),
                        row(int_entry('num', 2), bool_entry('active', false)),
                    )
                ),
            )
            ->write($loader)
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'num' => 1, 'active' => true],
                ['id' => 1, 'country' => 'PL', 'num' => 2, 'active' => false],
                ['id' => 2, 'country' => 'PL', 'num' => 1, 'active' => true],
                ['id' => 2, 'country' => 'PL', 'num' => 2, 'active' => false],
                ['id' => 3, 'country' => 'PL', 'num' => 1, 'active' => true],
                ['id' => 3, 'country' => 'PL', 'num' => 2, 'active' => false],
                ['id' => 4, 'country' => 'PL', 'num' => 1, 'active' => true],
                ['id' => 4, 'country' => 'PL', 'num' => 2, 'active' => false],
            ],
            $rows->toArray()
        );
    }
}
