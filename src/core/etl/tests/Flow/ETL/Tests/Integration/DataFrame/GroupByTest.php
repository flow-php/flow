<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Loader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class GroupByTest extends IntegrationTestCase
{
    public function test_group_by_multiple_columns_and_batch_size() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(4))
            ->method('load');

        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50), Entry::string('gender', 'male')),
            )
        )
            ->groupBy('country', 'gender')
            ->batchSize(1)
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'male')),
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'female')),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'female')),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'male')),
            ),
            $rows
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50), Entry::string('gender', 'male')),
            )
        )
            ->groupBy('country', 'gender')
            ->aggregate(average(ref('age')))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'male'), Entry::float('age_avg', 21.666666666666668)),
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'female'), Entry::integer('age_avg', 30)),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'female'), Entry::float('age_avg', 42.5)),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'male'), Entry::integer('age_avg', 45)),
            ),
            $rows
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation_with_null() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45), Entry::null('gender')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50), Entry::string('gender', 'male')),
            )
        )
            ->groupBy('country', 'gender')
            ->aggregate(average(ref('age')))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'male'), Entry::float('age_avg', 21.666666666666668)),
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'female'), Entry::integer('age_avg', 30)),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'female'), Entry::integer('age_avg', 40)),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'male'), Entry::integer('age_avg', 45)),
                Row::create(Entry::string('country', 'US'), Entry::null('gender'), Entry::integer('age_avg', 45)),
            ),
            $rows
        );
    }

    public function test_group_by_single_column() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->groupBy('country')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL')),
                Row::create(Entry::string('country', 'US')),
            ),
            $rows
        );
    }

    public function test_group_by_single_column_with_avg_aggregation() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->groupBy('country')
            ->aggregate(average(ref('age')))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL'), Entry::float('age_avg', 23.75)),
                Row::create(Entry::string('country', 'US'), Entry::float('age_avg', 43.75)),
            ),
            $rows
        );
    }

    public function test_group_by_twice() : void
    {
        $dataset = [
            ['date' => '2023-01-01', 'user' => 'user_01'],
            ['date' => '2023-01-01', 'user' => 'user_01'],
            ['date' => '2023-01-01', 'user' => 'user_02'],
            ['date' => '2023-01-01', 'user' => 'user_03'],
            ['date' => '2023-01-01', 'user' => 'user_01'],
            ['date' => '2023-01-02', 'user' => 'user_01'],
            ['date' => '2023-01-02', 'user' => 'user_02'],
            ['date' => '2023-01-02', 'user' => 'user_03'],
            ['date' => '2023-01-02', 'user' => 'user_03'],
            ['date' => '2023-01-03', 'user' => 'user_04'],
            ['date' => '2023-01-03', 'user' => 'user_04'],
            ['date' => '2023-01-03', 'user' => 'user_04'],
        ];

        $rows = (new Flow())
            ->read(From::array($dataset))
            ->groupBy(ref('date'), ref('user'))
            ->aggregate(count(ref('user')))
            ->rename('user_count', 'contributions')
            ->drop('date')
            ->groupBy(ref('user'))
            ->aggregate(sum(ref('contributions')))
            ->fetch();

        $this->assertSame(
            [
                ['user' => 'user_01', 'contributions_sum' => 4],
                ['user' => 'user_02', 'contributions_sum' => 2],
                ['user' => 'user_03', 'contributions_sum' => 3],
                ['user' => 'user_04', 'contributions_sum' => 3],
            ],
            $rows->toArray()
        );
    }

    public function test_pivot() : void
    {
        $dataset1 = [
            ['date' => '2023-11-01', 'user' => 'norberttech', 'contributions' => 5],
            ['date' => '2023-11-01', 'user' => 'stloyd', 'contributions' => 4],
            ['date' => '2023-11-02', 'user' => 'norberttech', 'contributions' => 3],
            ['date' => '2023-11-02', 'user' => 'stloyd', 'contributions' => 6],
        ];

        $dataset2 = [
            ['date' => '2023-11-03', 'user' => 'norberttech', 'contributions' => 2],
            ['date' => '2023-11-03', 'user' => 'stloyd', 'contributions' => 7],
            ['date' => '2023-11-04', 'user' => 'norberttech', 'contributions' => 3],
            ['date' => '2023-11-04', 'user' => 'stloyd', 'contributions' => 5],
            ['date' => '2023-11-05', 'user' => 'norberttech', 'contributions' => 7],
            ['date' => '2023-11-05', 'user' => 'stloyd', 'contributions' => 11],
        ];

        $rows = (new Flow())
            ->read(
                From::chain(
                    From::array($dataset1),
                    From::array($dataset2),
                )
            )
            ->groupBy(ref('date'))
            ->pivot(ref('user'))
            ->aggregate(sum(ref('contributions')))
            ->fetch();

        $this->assertSame(
            [
                [
                    'date' => '2023-11-01',
                    'norberttech' => 5,
                    'stloyd' => 4,
                ],
                [
                    'date' => '2023-11-02',
                    'norberttech' => 3,
                    'stloyd' => 6,
                ],
                [
                    'date' => '2023-11-03',
                    'norberttech' => 2,
                    'stloyd' => 7,
                ],
                [
                    'date' => '2023-11-04',
                    'norberttech' => 3,
                    'stloyd' => 5,
                ],
                [
                    'date' => '2023-11-05',
                    'norberttech' => 7,
                    'stloyd' => 11,
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_standalone_avg_aggregation() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->aggregate(average(ref('age')))
            ->rows(Transform::rename('age_avg', 'average_age'))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::float('average_age', 33.75)),
            ),
            $rows
        );
    }

    public function test_standalone_avg_and_max_aggregation() : void
    {
        (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->aggregate(average(ref('age')), max(ref('age')))
            ->run(function (Rows $rows) : void {
                $this->assertEquals(
                    new Rows(
                        Row::create(
                            Entry::float('age_avg', 33.75),
                            Entry::integer('age_max', 50)
                        )
                    ),
                    $rows
                );
            });
    }

    public function test_window_avg_function() : void
    {
        $memoryPage1 = new ArrayMemory([
            ['employee_name' => 'James', 'department' => 'Sales', 'salary' => 3000],
            ['employee_name' => 'Michael', 'department' => 'Sales', 'salary' => 4600],
            ['employee_name' => 'Jeff', 'department' => 'Marketing', 'salary' => 3000],
            ['employee_name' => 'Saif', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'John', 'department' => 'Marketing', 'salary' => 3200],
        ]);
        $memoryPage2 = new ArrayMemory([
            ['employee_name' => 'Emma', 'department' => 'Sales', 'salary' => 4800],
            ['employee_name' => 'Oliver', 'department' => 'Sales', 'salary' => 2900],
            ['employee_name' => 'Mia', 'department' => 'Finance', 'salary' => 3300],
            ['employee_name' => 'Noah', 'department' => 'Marketing', 'salary' => 3400],
            ['employee_name' => 'Ava', 'department' => 'Finance', 'salary' => 3800],
            ['employee_name' => 'Isabella', 'department' => 'Marketing', 'salary' => 2100],
            ['employee_name' => 'Ethan', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'Charlotte', 'department' => 'Marketing', 'salary' => 3000],
        ]);

        $this->assertSame(
            [
                ['department' => 'Sales', 'avg_salary' => 3917.0],
                ['department' => 'Marketing', 'avg_salary' => 2940.0],
                ['department' => 'Finance', 'avg_salary' => 3550.0],
            ],
            (new Flow)
                ->read(From::chain(From::memory($memoryPage1), From::memory($memoryPage2)))
                ->withEntry('avg_salary', average(ref('salary'))->over(window()->partitionBy(ref('department'))))
                ->select('department', 'avg_salary')
                ->dropDuplicates(ref('department'), ref('avg_salary'))
                ->withEntry('avg_salary', ref('avg_salary')->round(lit(0)))
                ->fetch()
                ->toArray()
        );
    }

    public function test_window_rank_function() : void
    {
        $memoryPage1 = new ArrayMemory([
            ['employee_name' => 'James', 'department' => 'Sales', 'salary' => 3000],
            ['employee_name' => 'James', 'department' => 'Sales', 'salary' => 3000],
            ['employee_name' => 'James', 'department' => 'Sales', 'salary' => 3000],
            ['employee_name' => 'Michael', 'department' => 'Sales', 'salary' => 4600],
            ['employee_name' => 'Robert', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'Maria', 'department' => 'Finance', 'salary' => 3000],
            ['employee_name' => 'Scott', 'department' => 'Finance', 'salary' => 3300],
            ['employee_name' => 'Jen', 'department' => 'Finance', 'salary' => 3900],
            ['employee_name' => 'Jeff', 'department' => 'Marketing', 'salary' => 3000],
            ['employee_name' => 'Kumar', 'department' => 'Marketing', 'salary' => 2000],
            ['employee_name' => 'Saif', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'John', 'department' => 'Marketing', 'salary' => 3200],
        ]);
        $memoryPage2 = new ArrayMemory([
            ['employee_name' => 'Emma', 'department' => 'Sales', 'salary' => 4800],
            ['employee_name' => 'Sophia', 'department' => 'Finance', 'salary' => 4200],
            ['employee_name' => 'Oliver', 'department' => 'Sales', 'salary' => 2900],
            ['employee_name' => 'Mia', 'department' => 'Finance', 'salary' => 3300],
            ['employee_name' => 'Noah', 'department' => 'Marketing', 'salary' => 3400],
            ['employee_name' => 'Ava', 'department' => 'Finance', 'salary' => 3800],
            ['employee_name' => 'Liam', 'department' => 'Sales', 'salary' => 3100],
            ['employee_name' => 'Isabella', 'department' => 'Marketing', 'salary' => 2100],
            ['employee_name' => 'Ethan', 'department' => 'Sales', 'salary' => 4100],
            ['employee_name' => 'Charlotte', 'department' => 'Marketing', 'salary' => 3000],
        ]);

        $this->assertSame(
            [
                ['employee_name' => 'Emma', 'department' => 'Sales', 'salary' => 4800, 'rank' => 1],
                ['employee_name' => 'Sophia', 'department' => 'Finance', 'salary' => 4200, 'rank' => 1],
                ['employee_name' => 'Noah', 'department' => 'Marketing', 'salary' => 3400, 'rank' => 1],
            ],
            (new Flow)
                ->read(From::all(From::memory($memoryPage1), From::memory($memoryPage2)))
                ->dropDuplicates(ref('employee_name'), ref('department'))
                ->withEntry('rank', rank()->over(window()->partitionBy(ref('department'))->orderBy(ref('salary')->desc())))
                ->filter(ref('rank')->equals(lit(1)))
                ->fetch()
                ->toArray()
        );
    }
}
