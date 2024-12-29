<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{average,
    count,
    datetime_entry,
    datetime_schema,
    df,
    float_entry,
    float_schema,
    from_all,
    from_array,
    from_memory,
    from_rows,
    int_entry,
    int_schema,
    json_entry,
    list_schema,
    lit,
    max,
    null_entry,
    rank,
    ref,
    row,
    rows,
    schema,
    str_entry,
    sum,
    type_list,
    type_string,
    uuid_entry,
    uuid_schema,
    window};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\{Loader, Rows};

final class GroupByTest extends FlowIntegrationTestCase
{
    public function test_group_by_array() : void
    {
        $rows = df()
            ->read(from_rows(rows(
                row(int_entry('id', 1), int_entry('score', 20), json_entry('array', ['a', 'b', 'c', 'd'])),
                row(int_entry('id', 2), int_entry('score', 20), json_entry('array', ['a', 'b', 'c', 'd'])),
                row(int_entry('id', 3), int_entry('score', 25), json_entry('array', ['a', 'b', 'c'])),
                row(int_entry('id', 4), int_entry('score', 30), json_entry('array', ['a', 'b', 'c'])),
                row(int_entry('id', 5), int_entry('score', 40), json_entry('array', ['a', 'b'])),
                row(int_entry('id', 6), int_entry('score', 40), json_entry('array', ['a', 'b'])),
                row(int_entry('id', 7), int_entry('score', 45), json_entry('array', ['a', 'b'])),
                row(int_entry('id', 9), int_entry('score', 50), json_entry('array', ['a'])),
            )))
            ->groupBy('array')
            ->aggregate(sum('score'), average('score'))
            ->fetch();

        self::assertEquals(
            schema(list_schema('array', type_list(type_string())), int_schema('score_sum'), float_schema('score_avg')),
            $rows->schema()
        );
        self::assertEquals(
            [
                ['array' => ['a', 'b', 'c', 'd'], 'score_sum' => 40, 'score_avg' => 20.0],
                ['array' => ['a', 'b', 'c'], 'score_sum' => 55, 'score_avg' => 27.5],
                ['array' => ['a', 'b'], 'score_sum' => 125, 'score_avg' => 41.666666666666664],
                ['array' => ['a'], 'score_sum' => 50, 'score_avg' => 50.0],
            ],
            $rows->toArray()
        );
    }

    public function test_group_by_date_time() : void
    {
        $rows = df()
            ->read(from_rows(rows(
                row(int_entry('id', 1), int_entry('score', 20), datetime_entry('date', '2024-01-01 10:00:00')),
                row(int_entry('id', 2), int_entry('score', 20), datetime_entry('date', '2024-01-01 10:00:00')),
                row(int_entry('id', 3), int_entry('score', 25), datetime_entry('date', '2024-01-02 10:00:00')),
                row(int_entry('id', 4), int_entry('score', 30), datetime_entry('date', '2024-01-02 10:00:00')),
                row(int_entry('id', 5), int_entry('score', 40), datetime_entry('date', '2024-01-03 10:00:00')),
                row(int_entry('id', 6), int_entry('score', 40), datetime_entry('date', '2024-01-03 10:00:00')),
                row(int_entry('id', 7), int_entry('score', 45), datetime_entry('date', '2024-01-03 10:00:00')),
                row(int_entry('id', 9), int_entry('score', 50), datetime_entry('date', '2024-01-04 10:00:00')),
            )))
            ->groupBy('date')
            ->aggregate(sum('score'), average('score'))
            ->fetch();

        self::assertEquals(
            schema(datetime_schema('date'), int_schema('score_sum'), float_schema('score_avg')),
            $rows->schema()
        );
        self::assertEquals(
            [
                ['date' => new \DateTimeImmutable('2024-01-01 10:00:00'), 'score_sum' => 40, 'score_avg' => 20.0],
                ['date' => new \DateTimeImmutable('2024-01-02 10:00:00'), 'score_sum' => 55, 'score_avg' => 27.5],
                ['date' => new \DateTimeImmutable('2024-01-03 10:00:00'), 'score_sum' => 125, 'score_avg' => 41.666666666666664],
                ['date' => new \DateTimeImmutable('2024-01-04 10:00:00'), 'score_sum' => 50, 'score_avg' => 50.0],
            ],
            $rows->toArray()
        );
    }

    public function test_group_by_multiple_columns_and_batch_size() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(4))
            ->method('load');

        $rows = df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25), str_entry('gender', 'male')),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30), str_entry('gender', 'female')),
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'female')),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'male')),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45), str_entry('gender', 'female')),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50), str_entry('gender', 'male')),
                )
            ))
            ->groupBy('country', 'gender')
            ->aggregate(average(ref('age')))
            ->withEntry('age_avg', ref('age_avg')->round(lit(2)))
            ->batchSize(1)
            ->write($loader)
            ->fetch();

        self::assertEquals(
            [
                ['country' => 'PL', 'gender' => 'male', 'age_avg' => 21.67],
                ['country' => 'PL', 'gender' => 'female', 'age_avg' => 30.0],
                ['country' => 'US', 'gender' => 'female', 'age_avg' => 42.5],
                ['country' => 'US', 'gender' => 'male', 'age_avg' => 45],
            ],
            $rows->toArray()
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25), str_entry('gender', 'male')),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30), str_entry('gender', 'female')),
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'female')),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'male')),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45), str_entry('gender', 'female')),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50), str_entry('gender', 'male')),
                )
            ))
            ->groupBy('country', 'gender')
            ->aggregate(average(ref('age')))
            ->fetch();

        self::assertEquals(
            rows(
                row(str_entry('country', 'PL'), str_entry('gender', 'male'), float_entry('age_avg', 21.666666666666668)),
                row(str_entry('country', 'PL'), str_entry('gender', 'female'), int_entry('age_avg', 30)),
                row(str_entry('country', 'US'), str_entry('gender', 'female'), float_entry('age_avg', 42.5)),
                row(str_entry('country', 'US'), str_entry('gender', 'male'), int_entry('age_avg', 45)),
            ),
            $rows
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation_with_null() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25), str_entry('gender', 'male')),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30), str_entry('gender', 'female')),
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'female')),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'male')),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45), str_entry('gender', null)),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50), str_entry('gender', 'male')),
                )
            ))
            ->groupBy('country', 'gender')
            ->aggregate(average(ref('age')))
            ->fetch();

        self::assertEquals(
            rows(
                row(str_entry('country', 'PL'), str_entry('gender', 'male'), float_entry('age_avg', 21.666666666666668)),
                row(str_entry('country', 'PL'), str_entry('gender', 'female'), int_entry('age_avg', 30)),
                row(str_entry('country', 'US'), str_entry('gender', 'female'), int_entry('age_avg', 40)),
                row(str_entry('country', 'US'), str_entry('gender', 'male'), int_entry('age_avg', 45)),
                row(str_entry('country', 'US'), null_entry('gender'), int_entry('age_avg', 45)),
            ),
            $rows
        );
    }

    public function test_group_by_single_column() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->groupBy('country')
            ->aggregate(sum(ref('age')))
            ->fetch();

        self::assertEquals(
            [
                ['country' => 'PL', 'age_sum' => 95],
                ['country' => 'US', 'age_sum' => 175],
            ],
            $rows->toArray()
        );
    }

    public function test_group_by_single_column_with_avg_aggregation() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->groupBy('country')
            ->aggregate(average(ref('age')))
            ->fetch();

        self::assertEquals(
            rows(
                row(str_entry('country', 'PL'), float_entry('age_avg', 23.75)),
                row(str_entry('country', 'US'), float_entry('age_avg', 43.75)),
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

        self::assertSame(
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
            ->read(from_rows(rows(
                row(int_entry('id', 1), int_entry('score', 20), uuid_entry('uuid', 'b97a23ab-ba84-4d8f-9d9a-abd32cc58110')),
                row(int_entry('id', 2), int_entry('score', 20), uuid_entry('uuid', 'b97a23ab-ba84-4d8f-9d9a-abd32cc58110')),
                row(int_entry('id', 3), int_entry('score', 25), uuid_entry('uuid', '28fc1a5f-25eb-40e2-88b8-7a0cdc5d18ae')),
                row(int_entry('id', 4), int_entry('score', 30), uuid_entry('uuid', '28fc1a5f-25eb-40e2-88b8-7a0cdc5d18ae')),
                row(int_entry('id', 5), int_entry('score', 40), uuid_entry('uuid', '5085fabf-15f7-4467-9076-61547afbbdc9')),
                row(int_entry('id', 6), int_entry('score', 40), uuid_entry('uuid', '5085fabf-15f7-4467-9076-61547afbbdc9')),
                row(int_entry('id', 7), int_entry('score', 45), uuid_entry('uuid', '5085fabf-15f7-4467-9076-61547afbbdc9')),
                row(int_entry('id', 9), int_entry('score', 50), uuid_entry('uuid', 'c7c22b40-45ad-46d1-a47b-0d1dd389ae41')),
            )))
            ->groupBy('uuid')
            ->aggregate(sum('score'), average('score'))
            ->fetch();

        self::assertEquals(
            schema(uuid_schema('uuid'), int_schema('score_sum'), float_schema('score_avg')),
            $rows->schema()
        );
        self::assertEquals(
            [
                ['uuid' => \Flow\ETL\PHP\Value\Uuid::fromString('b97a23ab-ba84-4d8f-9d9a-abd32cc58110'), 'score_sum' => 40, 'score_avg' => 20.0],
                ['uuid' => \Flow\ETL\PHP\Value\Uuid::fromString('28fc1a5f-25eb-40e2-88b8-7a0cdc5d18ae'), 'score_sum' => 55, 'score_avg' => 27.5],
                ['uuid' => \Flow\ETL\PHP\Value\Uuid::fromString('5085fabf-15f7-4467-9076-61547afbbdc9'), 'score_sum' => 125, 'score_avg' => 41.666666666666664],
                ['uuid' => \Flow\ETL\PHP\Value\Uuid::fromString('c7c22b40-45ad-46d1-a47b-0d1dd389ae41'), 'score_sum' => 50, 'score_avg' => 50.0],
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

        $rows = df()
            ->read(from_all(from_array($dataset1), from_array($dataset2)))
            ->groupBy(ref('date'))
            ->pivot(ref('user'))
            ->aggregate(sum(ref('contributions')))
            ->fetch();

        self::assertSame(
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
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->aggregate(average(ref('age')))
            ->rename('age_avg', 'average_age')
            ->fetch();

        self::assertEquals(
            rows(
                row(float_entry('average_age', 33.75)),
            ),
            $rows
        );
    }

    public function test_standalone_avg_and_max_aggregation() : void
    {
        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                    row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                    row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                    row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                    row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
                )
            ))
            ->aggregate(average(ref('age')), max(ref('age')))
            ->run(function (Rows $rows) : void {
                $this->assertEquals(
                    rows(
                        row(
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

        self::assertSame(
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

        self::assertSame(
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
