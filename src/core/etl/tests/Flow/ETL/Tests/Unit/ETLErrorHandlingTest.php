<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\{ignore_error_handler, skip_rows_handler, throw_error_handler};
use Flow\ETL\Row\Entry\{BooleanEntry, DateTimeEntry, IntegerEntry, NullEntry};
use Flow\ETL\{Extractor, Flow, FlowContext, Loader, Row, Rows, Transformer};
use PHPUnit\Framework\TestCase;

final class ETLErrorHandlingTest extends TestCase
{
    public function test_default_handler() : void
    {
        $extractor = new class implements Extractor {
            /**
             * @param FlowContext $context
             *
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract(FlowContext $context) : \Generator
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

        $brokenTransformer = new class implements Transformer {
            public function transform(Rows $rows, FlowContext $context) : Rows
            {
                throw new \RuntimeException('Transformer Exception');
            }
        };

        $loader = new class implements Loader {
            public array $result = [];

            public function load(Rows $rows, FlowContext $context) : void
            {
                $this->result = \array_merge($this->result, $rows->toArray());
            }
        };

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Transformer Exception');

        (new Flow())
            ->extract($extractor)
            ->onError(throw_error_handler())
            ->transform($brokenTransformer)
            ->load($loader)
            ->run();
    }

    public function test_ignore_error_handler() : void
    {
        $extractor = new class implements Extractor {
            /**
             * @param FlowContext $context
             *
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract(FlowContext $context) : \Generator
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

        $brokenTransformer = new class implements Transformer {
            public function transform(Rows $rows, FlowContext $context) : Rows
            {
                throw new \RuntimeException('Transformer Exception');
            }
        };

        $loader = new class implements Loader {
            public array $result = [];

            public function load(Rows $rows, FlowContext $context) : void
            {
                $this->result = \array_merge($this->result, $rows->toArray());
            }
        };

        (new Flow())
            ->extract($extractor)
            ->onError(ignore_error_handler())
            ->transform($brokenTransformer)
            ->load($loader)
            ->run();

        self::assertEquals(
            [
                [
                    'id' => 101,
                    'deleted' => false,
                    'expiration-date' => new \DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ],
                [
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new \DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ],
            ],
            $loader->result,
        );
    }

    public function test_skip_rows_handler() : void
    {
        $extractor = new class implements Extractor {
            /**
             * @param FlowContext $context
             *
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract(FlowContext $context) : \Generator
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

        $brokenTransformer = new class implements Transformer {
            public function transform(Rows $rows, FlowContext $context) : Rows
            {
                if ($rows->first()->valueOf('id') === 101) {
                    throw new \RuntimeException('Transformer Exception');
                }

                return $rows;
            }
        };

        $loader = new class implements Loader {
            public array $result = [];

            public function load(Rows $rows, FlowContext $context) : void
            {
                $this->result = \array_merge($this->result, $rows->toArray());
            }
        };

        (new Flow())
            ->extract($extractor)
            ->onError(skip_rows_handler())
            ->transform($brokenTransformer)
            ->load($loader)
            ->run();

        self::assertEquals(
            [
                [
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new \DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ],
            ],
            $loader->result,
        );
    }
}
