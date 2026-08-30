<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Generator;

use function array_map;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_structure;
use function range;

final class OffsetTest extends FlowIntegrationTestCase
{
    public function test_limit_with_offset(): void
    {
        $rows = df()
            ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 20))))
            ->limit(10)
            ->offset(5)
            ->fetch();
        static::assertCount(5, $rows);
        static::assertSame(
            [
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ],
            $rows->toArray(),
        );
    }

    public function test_offset_constructor_with_negative_offset(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0, given: -1');
        // @mago-ignore analysis:invalid-argument
        df()->read(from_rows(rows(schema())))->offset(-1);
    }

    public function test_offset_null(): void
    {
        $rows = df()
            ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 10))))
            ->offset(null)
            ->fetch();
        static::assertCount(10, $rows);
    }

    public function test_offset_skipping_all_rows(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]))
            ->offset(3)
            ->fetch();
        static::assertCount(0, $rows);
    }

    public function test_offset_skipping_more_rows_than_available(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
            ]))
            ->offset(5)
            ->fetch();
        static::assertCount(0, $rows);
    }

    public function test_offset_skipping_some_rows(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ]))
            ->offset(2)
            ->fetch();
        static::assertCount(3, $rows);
        static::assertSame(
            [
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ],
            $rows->toArray(),
        );
    }

    public function test_offset_with_batch_size(): void
    {
        $rows = df()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return new Schema();
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    for ($i = 0; $i < 10; $i++) {
                        yield rows(schema(integer_schema('id')), row(['id' => $i + 1]));
                    }
                }
            })
            ->batchSize(3)
            ->offset(4)
            ->fetch();
        static::assertCount(6, $rows);
        static::assertSame(
            [
                ['id' => 5],
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ],
            $rows->toArray(),
        );
    }

    public function test_offset_with_collect(): void
    {
        $rows = df()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return new Schema();
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    for ($i = 0; $i < 5; $i++) {
                        yield rows(schema(integer_schema('id')), row(['id' => $i + 1]));
                    }
                }
            })
            ->offset(2)
            ->collect()
            ->fetch();
        static::assertCount(3, $rows);
        static::assertSame(
            [
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ],
            $rows->toArray(),
        );
    }

    public function test_offset_with_expanding_transformations(): void
    {
        $rows = df()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return new Schema();
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    for ($i = 0; $i < 100; $i++) {
                        yield rows(
                            schema(list_schema(
                                'ids',
                                type_list(type_structure([
                                    'id' => type_integer(),
                                ])),
                            )),
                            row([
                                'ids' => [
                                    ['id' => $i + 1],
                                    ['id' => $i + 2],
                                    ['id' => $i + 3],
                                ],
                            ]),
                        );
                    }
                }
            })
            ->withEntries([
                'expanded' => ref('ids')->expand(),
                'element' => ref('expanded')->unpack(),
            ])
            ->rename('element.id', 'id')
            ->drop('expanded', 'ids', 'element')
            ->offset(5)
            ->limit(3)
            ->fetch();
        static::assertCount(3, $rows);
    }

    public function test_offset_with_limit(): void
    {
        $rows = df()
            ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 20))))
            ->offset(5)
            ->limit(10)
            ->fetch();
        static::assertCount(10, $rows);
        static::assertSame(
            [
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
                ['id' => 11],
                ['id' => 12],
                ['id' => 13],
                ['id' => 14],
                ['id' => 15],
            ],
            $rows->toArray(),
        );
    }

    public function test_offset_with_multiple_batches(): void
    {
        $rows = df()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return new Schema();
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    for ($i = 0; $i < 10; $i++) {
                        yield rows(schema(integer_schema('id')), row(['id' => $i + 1]));
                    }
                }
            })
            ->offset(4)
            ->fetch();
        static::assertCount(6, $rows);
        static::assertSame(
            [
                ['id' => 5],
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ],
            $rows->toArray(),
        );
    }

    public function test_offset_with_transformations(): void
    {
        $rows = df()
            ->read(from_array(array_map(static fn(int $id): array => ['id' => $id, 'value' => $id * 2], range(1, 10))))
            ->withEntry('sum', ref('id')->plus(ref('value')))
            ->offset(3)
            ->fetch();
        static::assertCount(7, $rows);
        static::assertSame(
            [
                ['id' => 4, 'value' => 8, 'sum' => 12],
                ['id' => 5, 'value' => 10, 'sum' => 15],
                ['id' => 6, 'value' => 12, 'sum' => 18],
                ['id' => 7, 'value' => 14, 'sum' => 21],
                ['id' => 8, 'value' => 16, 'sum' => 24],
                ['id' => 9, 'value' => 18, 'sum' => 27],
                ['id' => 10, 'value' => 20, 'sum' => 30],
            ],
            $rows->toArray(),
        );
    }

    public function test_offset_with_zero(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]))
            ->offset(0)
            ->fetch();
        static::assertCount(3, $rows);
        static::assertSame(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            $rows->toArray(),
        );
    }

    public function test_pagination_scenario(): void
    {
        $data = array_map(static fn(int $id): array => ['id' => $id, 'name' => 'Item ' . $id], range(1, 100));
        $page1 = df()->read(from_array($data))->offset(0)->limit(10)->fetch();
        static::assertCount(10, $page1);
        static::assertSame(1, $page1->first()->get('id'));
        static::assertSame(10, $page1->all()[9]->get('id'));
        $page3 = df()->read(from_array($data))->offset(20)->limit(10)->fetch();
        static::assertCount(10, $page3);
        static::assertSame(21, $page3->first()->get('id'));
        static::assertSame(30, $page3->all()[9]->get('id'));
    }
}
