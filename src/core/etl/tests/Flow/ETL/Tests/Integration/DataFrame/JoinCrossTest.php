<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Runtime\ErrorHandler\SkipRows;
use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RecordingErrorHandler;
use Flow\ETL\Tests\Double\ThrowingAfterFirstBatchExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use RuntimeException;
use SensitiveParameter;

use function array_filter;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinCrossTest extends FlowTestCase
{
    public function test_cross_join(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::exactly(2))->method('load');

        $rows = df()
            ->read(from_rows(array_to_rows(
                [
                    ['id' => 1, 'country' => 'PL'],
                    ['id' => 2, 'country' => 'PL'],
                    ['id' => 3, 'country' => 'PL'],
                    ['id' => 4, 'country' => 'PL'],
                ],
                schema(int_schema('id'), str_schema('country')),
            )))
            ->batchSize(2)
            ->crossJoin(data_frame()->process(array_to_rows(
                [['num' => 1, 'active' => true], ['num' => 2, 'active' => false]],
                schema(int_schema('num'), bool_schema('active')),
            )))
            ->write($loader)
            ->fetch();

        static::assertEquals(
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
            $rows->toArray(),
        );
    }

    public function test_a_failing_right_side_propagates_without_reaching_the_transformation_handler(): void
    {
        $handler = new RecordingErrorHandler(new ThrowError());

        try {
            df()
                ->read(from_rows(array_to_rows([['country' => 'PL']], schema(str_schema('country')))))
                ->crossJoin(df()->read(new ThrowingAfterFirstBatchExtractor()))
                ->onError($handler)
                ->fetch();

            static::fail('The failing right side must propagate');
        } catch (RuntimeException $e) {
            static::assertSame('source failed after its first batch', $e->getMessage());
        }

        static::assertSame(
            [],
            array_filter($handler->errors, static fn(object $error): bool => $error instanceof TransformationError),
        );
    }

    public function test_a_failing_join_of_a_left_batch_propagates_without_reaching_the_transformation_handler(): void
    {
        $handler = new RecordingErrorHandler(new SkipRows());
        // binds its input unchanged, then emits a column the right side holds too: the join fails on the batch
        $hiddenColumn = new class implements Transformer {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('never described');
            }

            public function describe(): ?Description
            {
                return null;
            }

            public function bind(Schema $input): BoundStep
            {
                return new BoundStep($this, $input);
            }

            public function transform(Rows $rows, Backend $backend): Rows
            {
                return array_to_rows([['id' => 1, 'num' => 1]], schema(int_schema('id'), int_schema('num')));
            }
        };

        try {
            df()
                ->read(from_rows(array_to_rows([['id' => 1]], schema(int_schema('id')))))
                ->with($hiddenColumn)
                ->crossJoin(df()->read(from_rows(array_to_rows([['num' => 2]], schema(int_schema('num'))))))
                ->onError($handler)
                ->fetch();

            static::fail('The failing join must propagate');
        } catch (SchemaDefinitionNotUniqueException) {
        }

        static::assertSame(
            [],
            array_filter($handler->errors, static fn(object $error): bool => $error instanceof TransformationError),
        );
    }

    public function test_the_runtime_reads_the_right_plan_once_over_every_left_batch(): void
    {
        $right = new CountingExtractor(
            schema(int_schema('num')),
            array_to_rows([['num' => 1]], schema(int_schema('num'))),
        );

        static::assertCount(
            3,
            df()
                ->read((new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))->withBatchSize(
                    1,
                ))
                ->crossJoin(df()->read($right))
                ->fetch(),
        );
        static::assertSame(1, $right->extractCalls);
    }

    public function test_an_empty_left_side_never_reads_the_right_plan(): void
    {
        $right = new CountingExtractor(
            schema(int_schema('num')),
            array_to_rows([['num' => 1]], schema(int_schema('num'))),
        );

        static::assertCount(
            0,
            df()
                ->read(from_rows(rows(schema(int_schema('id')))))
                ->crossJoin(df()->read($right))
                ->fetch(),
        );
        static::assertSame(0, $right->extractCalls);
    }
}
