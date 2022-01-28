<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\ETL;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\Transformer;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

final class ETLTest extends TestCase
{
    public function test_etl() : void
    {
        $extractor = new class implements Extractor {
            /**
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract() : \Generator
            {
                yield new Rows(
                    Row::create(
                        new IntegerEntry('id', 101),
                        new BooleanEntry('deleted', false),
                        new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
                        new NullEntry('phase')
                    )
                );

                yield new Rows(
                    Row::create(
                        new IntegerEntry('id', 102),
                        new BooleanEntry('deleted', true),
                        new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-25')),
                        new NullEntry('phase')
                    )
                );
            }
        };

        $addStampStringEntry = new class implements Transformer {
            public function transform(Rows $rows) : Rows
            {
                return $rows->map(
                    fn (Row $row) : Row => $row->set(new StringEntry('stamp', 'zero'))
                );
            }
        };

        $loader = new class implements Loader {
            public array $result = [];

            public function load(Rows $rows) : void
            {
                $this->result = \array_merge($this->result, $rows->toArray());
            }
        };

        ETL::extract($extractor)
            ->onError(new IgnoreError())
            ->transform($addStampStringEntry)
            ->transform(new class implements Transformer {
                public function transform(Rows $rows) : Rows
                {
                    throw new \RuntimeException('Unexpected exception');
                }
            })
            ->transform(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'one'))
            ->transform(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'two'))
            ->transform(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'three'))
            ->load($loader)
            ->run();

        $this->assertEquals(
            [
                [
                    'id' => 101,
                    'stamp' => 'zero:one:two:three',
                    'deleted' => false,
                    'expiration-date' => new \DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ],
                [
                    'id' => 102,
                    'stamp' => 'zero:one:two:three',
                    'deleted' => true,
                    'expiration-date' => new \DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ],
            ],
            $loader->result,
        );
    }

    public function test_first_and_last_rows() : void
    {
        $callback = function (int $index, Rows $rows) : void {
            if ($index === 0) {
                Assert::assertTrue($rows->isFirst());
            }

            if ($index === 3) {
                Assert::assertTrue($rows->isLast());
            }
        };

        ETL::extract(new class implements Extractor {
            /**
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract() : \Generator
            {
                yield new Rows(Row::create(new IntegerEntry('id', 1)));
                yield new Rows(Row::create(new IntegerEntry('id', 2)));
                yield new Rows(Row::create(new IntegerEntry('id', 3)));
            }
        })
            ->onError(new ThrowError())
            ->load(new class($callback) implements Loader {
                private int $index = 0;

                private $callback;

                public function __construct(callable $callback)
                {
                    $this->callback = $callback;
                }

                public function load(Rows $rows) : void
                {
                    ($this->callback)($this->index, $rows);
                    $this->index++;
                }
            })->run();
    }

    public function test_etl_with_collecting() : void
    {
        ETL::extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    yield new Rows(Row::create(new IntegerEntry('id', 1)));
                    yield new Rows(Row::create(new IntegerEntry('id', 2)));
                    yield new Rows(Row::create(new IntegerEntry('id', 3)));
                }
            }
        )
            ->transform(
                new class implements Transformer {
                    public function transform(Rows $rows) : Rows
                    {
                        return $rows->map(fn (Row $row) => $row->rename('id', 'new_id'));
                    }
                }
            )
            ->collect()
            ->load(
                new class implements Loader {
                    public function load(Rows $rows) : void
                    {
                        Assert::assertCount(3, $rows);
                        Assert::assertTrue($rows->isFirst());
                        Assert::assertTrue($rows->isLast());
                    }
                }
            )
            ->run();
    }

    public function test_etl_with_parallelizing() : void
    {
        ETL::extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    yield new Rows(
                        Row::create(new IntegerEntry('id', 1)),
                        Row::create(new IntegerEntry('id', 2)),
                        Row::create(new IntegerEntry('id', 3)),
                        Row::create(new IntegerEntry('id', 4)),
                        Row::create(new IntegerEntry('id', 5)),
                        Row::create(new IntegerEntry('id', 6)),
                        Row::create(new IntegerEntry('id', 7)),
                        Row::create(new IntegerEntry('id', 8)),
                        Row::create(new IntegerEntry('id', 9)),
                        Row::create(new IntegerEntry('id', 10)),
                    );
                }
            }
        )
            ->transform(
                new class implements Transformer {
                    public function transform(Rows $rows) : Rows
                    {
                        return $rows->map(fn (Row $row) => $row->rename('id', 'new_id'));
                    }
                }
            )
            ->parallelize(2)
            ->load(
                new class implements Loader {
                    public function load(Rows $rows) : void
                    {
                        Assert::assertCount(2, $rows);
                    }
                }
            )
            ->run();
    }
}
