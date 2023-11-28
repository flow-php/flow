<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\read;
use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\DSL\Entry;
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

        $rows = read(from_rows(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL')),
            )
        ))
            ->batchSize(2)
            ->crossJoin(
                (new Flow())->process(
                    new Rows(
                        Row::create(Entry::integer('num', 1), Entry::boolean('active', true)),
                        Row::create(Entry::integer('num', 2), Entry::boolean('active', false)),
                    )
                ),
            )
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('num', 1), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('num', 2), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('num', 1), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('num', 2), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('num', 1), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('num', 2), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('num', 1), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('num', 2), Entry::boolean('active', false)),
            ),
            $rows
        );
    }

    public function test_join() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(2))
            ->method('load');

        $rows = read(from_rows(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US')),
            )
        ))
            ->batchSize(4)
            ->join(
                (new Flow())->process(
                    new Rows(
                        Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                        Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                    )
                ),
                Expression::on(['country' => 'code']),
            )
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::string('name', 'United States')),
            ),
            $rows
        );
    }

    public function test_join_each() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(2))
            ->method('load');

        $rows = read(from_rows(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US')),
            )
        ))
            ->batchSize(4)
            ->joinEach(
                new class implements DataFrameFactory {
                    public function from(Rows $rows) : DataFrame
                    {
                        return (new Flow())->process(
                            new Rows(
                                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                            )
                        );
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                },
                Expression::on(['country' => 'code']),
            )
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::string('name', 'United States')),
            ),
            $rows
        );
    }
}
