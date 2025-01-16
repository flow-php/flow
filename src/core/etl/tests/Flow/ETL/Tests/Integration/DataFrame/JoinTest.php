<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{datetime_entry, df, from_rows, int_entry, row, rows, str_entry};
use Flow\ETL\Join\Expression;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\{Join\Join, Loader};

final class JoinTest extends FlowIntegrationTestCase
{
    public function test_join_inner() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))
            ->method('load');

        $rows = df()
            ->from(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL')),
                    row(int_entry('id', 2), str_entry('country', 'US')),
                    row(int_entry('id', 3), str_entry('country', 'FR')),
                    row(int_entry('id', 4), str_entry('country', 'UK')),
                    row(int_entry('id', 5), str_entry('country', 'GB')),
                )
            ))
            ->join(
                (data_frame())->process(
                    rows(
                        row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                        row(str_entry('code', 'US'), str_entry('name', 'United States')),
                        row(str_entry('code', 'FR'), str_entry('name', 'France')),
                        row(str_entry('code', 'CN'), str_entry('name', 'Canada')),
                    )
                ),
                Expression::on(['country' => 'code']),
                Join::inner
            )
            ->batchSize(2)
            ->write($loader)
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'joined_name' => 'Poland', 'country' => 'PL', 'joined_code' => 'PL'],
                ['id' => 2, 'joined_name' => 'United States', 'country' => 'US', 'joined_code' => 'US'],
                ['id' => 3, 'joined_name' => 'France', 'country' => 'FR', 'joined_code' => 'FR'],
            ],
            $rows->toArray()
        );
    }

    public function test_join_left() : void
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
                    row(int_entry('id', 5), str_entry('country', 'US')),
                    row(int_entry('id', 6), str_entry('country', 'US')),
                    row(int_entry('id', 7), str_entry('country', 'US')),
                    row(int_entry('id', 9), str_entry('country', 'US')),
                )
            ))
            ->join(
                (data_frame())->process(
                    rows(
                        row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                        row(str_entry('code', 'US'), str_entry('name', 'United States')),
                    )
                ),
                Expression::on(['country' => 'code']),
            )
            ->batchSize(4)
            ->write($loader)
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 2, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 3, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 4, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 5, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
                ['id' => 6, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
                ['id' => 7, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
                ['id' => 9, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
            ],
            $rows->toArray()
        );
    }

    public function test_join_left_anti() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(1))
            ->method('load');

        $rows = df()
            ->from(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL')),
                    row(int_entry('id', 2), str_entry('country', 'US')),
                    row(int_entry('id', 3), str_entry('country', 'FR')),
                    row(int_entry('id', 5), str_entry('country', 'GB')),
                    row(int_entry('id', 7), str_entry('country', 'CN')),
                )
            ))
            ->join(
                (data_frame())->process(
                    rows(
                        row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                        row(str_entry('code', 'US'), str_entry('name', 'United States')),
                        row(str_entry('code', 'FR'), str_entry('name', 'France')),
                        row(str_entry('code', 'CA'), str_entry('name', 'Canada')),
                    )
                ),
                Expression::on(['country' => 'code']),
                Join::left_anti
            )
            ->batchSize(2)
            ->write($loader)
            ->fetch();

        self::assertEquals(
            [
                ['id' => 5, 'country' => 'GB'],
                ['id' => 7, 'country' => 'CN'],
            ],
            $rows->toArray()
        );
    }

    public function test_join_left_on_date_time_entry() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))
            ->method('load');

        $rows = df()
            ->from(from_rows(
                rows(
                    row(int_entry('id', 1), datetime_entry('date', new \DateTimeImmutable('2024-01-01 00:00:00'))),
                    row(int_entry('id', 2), datetime_entry('date', new \DateTimeImmutable('2024-01-01 00:00:00'))),
                    row(int_entry('id', 3), datetime_entry('date', new \DateTimeImmutable('2024-01-02 00:00:00'))),
                    row(int_entry('id', 4), datetime_entry('date', new \DateTimeImmutable('2024-01-03 00:00:00'))),
                    row(int_entry('id', 5), datetime_entry('date', new \DateTimeImmutable('2024-01-04 00:00:00'))),
                    row(int_entry('id', 6), datetime_entry('date', new \DateTimeImmutable('2024-01-04 00:00:00'))),
                    row(int_entry('id', 7), datetime_entry('date', new \DateTimeImmutable('2024-01-05 00:00:00'))),
                    row(int_entry('id', 9), datetime_entry('date', new \DateTimeImmutable('2024-01-05 00:00:00'))),
                )
            ))
            ->join(
                (data_frame())->process(
                    rows(
                        row(datetime_entry('date', new \DateTimeImmutable('2024-01-01 00:00:00')), int_entry('events', 1)),
                        row(datetime_entry('date', new \DateTimeImmutable('2024-01-05 00:00:00')), int_entry('events', 5)),
                    )
                ),
                Expression::on(['date' => 'date']),
                Join::left
            )
            ->batchSize(4)
            ->write($loader)
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'date' => new \DateTimeImmutable('2024-01-01 00:00:00'), 'joined_date' => new \DateTimeImmutable('2024-01-01 00:00:00'), 'joined_events' => 1],
                ['id' => 2, 'date' => new \DateTimeImmutable('2024-01-01 00:00:00'), 'joined_date' => new \DateTimeImmutable('2024-01-01 00:00:00'), 'joined_events' => 1],
                ['id' => 3, 'date' => new \DateTimeImmutable('2024-01-02 00:00:00'), 'joined_date' => null, 'joined_events' => null],
                ['id' => 4, 'date' => new \DateTimeImmutable('2024-01-03 00:00:00'), 'joined_date' => null, 'joined_events' => null],
                ['id' => 5, 'date' => new \DateTimeImmutable('2024-01-04 00:00:00'), 'joined_date' => null, 'joined_events' => null],
                ['id' => 6, 'date' => new \DateTimeImmutable('2024-01-04 00:00:00'), 'joined_date' => null, 'joined_events' => null],
                ['id' => 7, 'date' => new \DateTimeImmutable('2024-01-05 00:00:00'), 'joined_date' => new \DateTimeImmutable('2024-01-05 00:00:00'), 'joined_events' => 5],
                ['id' => 9, 'date' => new \DateTimeImmutable('2024-01-05 00:00:00'), 'joined_date' => new \DateTimeImmutable('2024-01-05 00:00:00'), 'joined_events' => 5],
            ],
            $rows->toArray()
        );
    }

    public function test_join_on_same_column_name() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))
            ->method('load');

        $rows = df()
            ->from(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('code', 'PL')),
                    row(int_entry('id', 2), str_entry('code', 'PL')),
                    row(int_entry('id', 3), str_entry('code', 'PL')),
                    row(int_entry('id', 4), str_entry('code', 'PL')),
                    row(int_entry('id', 5), str_entry('code', 'US')),
                    row(int_entry('id', 6), str_entry('code', 'US')),
                    row(int_entry('id', 7), str_entry('code', 'US')),
                    row(int_entry('id', 9), str_entry('code', 'US')),
                )
            ))
            ->join(
                (data_frame())->process(
                    rows(
                        row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                        row(str_entry('code', 'US'), str_entry('name', 'United States')),
                    )
                ),
                Expression::on(['code' => 'code']),
            )
            ->batchSize(4)
            ->write($loader)
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'code' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'code' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 3, 'code' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 4, 'code' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 5, 'code' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 6, 'code' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 7, 'code' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 9, 'code' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
            ],
            $rows->toArray()
        );
    }

    public function test_join_right() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))
            ->method('load');

        $rows = df()
            ->from(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL')),
                    row(int_entry('id', 2), str_entry('country', 'US')),
                    row(int_entry('id', 3), str_entry('country', 'FR')),
                    row(int_entry('id', 4), str_entry('country', 'UK')),
                    row(int_entry('id', 5), str_entry('country', 'GB')),
                )
            ))
            ->join(
                (data_frame())->process(
                    rows(
                        row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                        row(str_entry('code', 'US'), str_entry('name', 'United States')),
                        row(str_entry('code', 'FR'), str_entry('name', 'France')),
                        row(str_entry('code', 'CA'), str_entry('name', 'Canada')),
                    )
                ),
                Expression::on(['country' => 'code']),
                Join::right
            )
            ->batchSize(2)
            ->write($loader)
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'joined_code' => 'PL', 'joined_name' => 'Poland', 'country' => 'PL'],
                ['id' => 2, 'joined_code' => 'US', 'joined_name' => 'United States', 'country' => 'US'],
                ['id' => 3, 'joined_code' => 'FR', 'joined_name' => 'France', 'country' => 'FR'],
                ['id' => null, 'joined_code' => 'CA', 'joined_name' => 'Canada', 'country' => null],
            ],
            $rows->toArray()
        );
    }
}
