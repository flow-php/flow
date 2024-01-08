<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_entry;
use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Flow;
use Flow\ETL\Join\Expression;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class JoinTest extends IntegrationTestCase
{
    public function test_cross_join() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(2))
            ->method('load');

        $rows = df()
            ->from(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL')),
                )
            ))
            ->batchSize(2)
            ->crossJoin(
                (new Flow())->process(
                    new Rows(
                        Row::create(int_entry('num', 1), bool_entry('active', true)),
                        Row::create(int_entry('num', 2), bool_entry('active', false)),
                    )
                ),
            )
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('num', 1), bool_entry('active', true)),
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('num', 2), bool_entry('active', false)),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('num', 1), bool_entry('active', true)),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('num', 2), bool_entry('active', false)),
                Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('num', 1), bool_entry('active', true)),
                Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('num', 2), bool_entry('active', false)),
                Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('num', 1), bool_entry('active', true)),
                Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('num', 2), bool_entry('active', false)),
            ),
            $rows
        );
    }

    public function test_join() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(2))
            ->method('load');

        $rows = df()
            ->from(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 5), str_entry('country', 'US')),
                    Row::create(int_entry('id', 6), str_entry('country', 'US')),
                    Row::create(int_entry('id', 7), str_entry('country', 'US')),
                    Row::create(int_entry('id', 9), str_entry('country', 'US')),
                )
            ))
            ->batchSize(4)
            ->join(
                (new Flow())->process(
                    new Rows(
                        Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                        Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                    )
                ),
                Expression::on(['country' => 'code']),
            )
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), string_entry('country', 'PL'), string_entry('name', 'Poland')),
                Row::create(int_entry('id', 2), string_entry('country', 'PL'), string_entry('name', 'Poland')),
                Row::create(int_entry('id', 3), string_entry('country', 'PL'), string_entry('name', 'Poland')),
                Row::create(int_entry('id', 4), string_entry('country', 'PL'), string_entry('name', 'Poland')),
                Row::create(int_entry('id', 5), string_entry('country', 'US'), string_entry('name', 'United States')),
                Row::create(int_entry('id', 6), string_entry('country', 'US'), string_entry('name', 'United States')),
                Row::create(int_entry('id', 7), string_entry('country', 'US'), string_entry('name', 'United States')),
                Row::create(int_entry('id', 9), string_entry('country', 'US'), string_entry('name', 'United States')),
            ),
            $rows
        );
    }

    public function test_join_each() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(2))
            ->method('load');

        $rows = df()
            ->read(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL')),
                    Row::create(int_entry('id', 5), str_entry('country', 'US')),
                    Row::create(int_entry('id', 6), str_entry('country', 'US')),
                    Row::create(int_entry('id', 7), str_entry('country', 'US')),
                    Row::create(int_entry('id', 9), str_entry('country', 'US')),
                )
            ))
            ->batchSize(4)
            ->joinEach(
                new class implements DataFrameFactory {
                    public function from(Rows $rows) : DataFrame
                    {
                        return (new Flow())->process(
                            new Rows(
                                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                            )
                        );
                    }
                },
                Expression::on(['country' => 'code']),
            )
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 3), str_entry('country', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 4), str_entry('country', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 5), str_entry('country', 'US'), str_entry('name', 'United States')),
                Row::create(int_entry('id', 6), str_entry('country', 'US'), str_entry('name', 'United States')),
                Row::create(int_entry('id', 7), str_entry('country', 'US'), str_entry('name', 'United States')),
                Row::create(int_entry('id', 9), str_entry('country', 'US'), str_entry('name', 'United States')),
            ),
            $rows
        );
    }
}
