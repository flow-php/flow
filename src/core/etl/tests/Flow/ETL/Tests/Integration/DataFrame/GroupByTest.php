<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_memory;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use Flow\ETL\Loader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Ramsey\Uuid\Uuid;

final class GroupByTest extends IntegrationTestCase
{
    public function test_group_by_multiple_columns_and_batch_size() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(4))
            ->method('load');

        $rows = df()
            ->read(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30), str_entry('gender', 'female')),
                    Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'female')),
                    Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45), str_entry('gender', 'female')),
                    Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50), str_entry('gender', 'male')),
                )
            ))
            ->groupBy('country', 'gender')
            ->batchSize(1)
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('country', 'PL'), str_entry('gender', 'male')),
                Row::create(str_entry('country', 'PL'), str_entry('gender', 'female')),
                Row::create(str_entry('country', 'US'), str_entry('gender', 'female')),
                Row::create(str_entry('country', 'US'), str_entry('gender', 'male')),
            ),
            $rows
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation() : void
    {
        $rows = df()
            ->read(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30), str_entry('gender', 'female')),
                    Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'female')),
                    Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45), str_entry('gender', 'female')),
                    Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50), str_entry('gender', 'male')),
                )
            ))
            ->groupBy('country', 'gender')
            ->aggregate(average(ref('age')))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('country', 'PL'), str_entry('gender', 'male'), float_entry('age_avg', 21.666666666666668)),
                Row::create(str_entry('country', 'PL'), str_entry('gender', 'female'), int_entry('age_avg', 30)),
                Row::create(str_entry('country', 'US'), str_entry('gender', 'female'), float_entry('age_avg', 42.5)),
                Row::create(str_entry('country', 'US'), str_entry('gender', 'male'), int_entry('age_avg', 45)),
            ),
            $rows
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation_with_null() : void
    {
        $rows = df()
            ->read(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30), str_entry('gender', 'female')),
                    Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'female')),
                    Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'male')),
                    Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45), null_entry('gender')),
                    Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50), str_entry('gender', 'male')),
                )
            ))
            ->groupBy('country', 'gender')
            ->aggregate(average(ref('age')))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('country', 'PL'), str_entry('gender', 'male'), float_entry('age_avg', 21.666666666666668)),
                Row::create(str_entry('country', 'PL'), str_entry('gender', 'female'), int_entry('age_avg', 30)),
                Row::create(str_entry('country', 'US'), str_entry('gender', 'female'), int_entry('age_avg', 40)),
                Row::create(str_entry('country', 'US'), str_entry('gender', 'male'), int_entry('age_avg', 45)),
                Row::create(str_entry('country', 'US'), null_entry('gender'), int_entry('age_avg', 45)),
            ),
            $rows
        );
    }

    public function test_group_by_single_column() : void
    {
        $rows = df()
            ->read(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->groupBy('country')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('country', 'PL')),
                Row::create(str_entry('country', 'US')),
            ),
            $rows
        );
    }

    public function test_group_by_single_column_with_avg_aggregation() : void
    {
        $rows = df()
            ->read(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->groupBy('country')
            ->aggregate(average(ref('age')))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('country', 'PL'), float_entry('age_avg', 23.75)),
                Row::create(str_entry('country', 'US'), float_entry('age_avg', 43.75)),
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

        $rows = df()
            ->read(from_array($dataset))
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

    public function test_group_by_uuid() : void
    {
        $rows = df()
            ->read(from_array([
                ['id' => Uuid::uuid4()->toString()],
                ['id' => Uuid::uuid4()->toString()],
                ['id' => Uuid::uuid4()->toString()],
                ['id' => Uuid::uuid4()->toString()],
                ['id' => Uuid::uuid4()->toString()],
                ['id' => Uuid::uuid4()->toString()],
                ['id' => Uuid::uuid4()->toString()],
                ['id' => Uuid::uuid4()->toString()],
            ]))
            ->aggregate(count(ref('id')))
            ->fetch();

        $this->assertEquals(
            new Rows(Row::create(integer_entry('id_count', 8))),
            $rows
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

        $rows = df()
            ->read(from_all(from_array($dataset1), from_array($dataset2)))
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
        $rows = df()
            ->read(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->aggregate(average(ref('age')))
            ->rename('age_avg', 'average_age')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(float_entry('average_age', 33.75)),
            ),
            $rows
        );
    }

    public function test_standalone_avg_and_max_aggregation() : void
    {
        df()
            ->read(from_rows(
                new Rows(
                    Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->aggregate(average(ref('age')), max(ref('age')))
            ->run(function (Rows $rows) : void {
                $this->assertEquals(
                    new Rows(
                        Row::create(
                            float_entry('age_avg', 33.75),
                            int_entry('age_max', 50)
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
            df()
                ->from(from_all(from_memory($memoryPage1), from_memory($memoryPage2)))
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
            df()
                ->from(from_all(from_memory($memoryPage1), from_memory($memoryPage2)))
                ->dropDuplicates(ref('employee_name'), ref('department'))
                ->withEntry('rank', rank()->over(window()->partitionBy(ref('department'))->orderBy(ref('salary')->desc())))
                ->filter(ref('rank')->equals(lit(1)))
                ->fetch()
                ->toArray()
        );
    }
}
