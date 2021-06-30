<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\SkipRows;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\ETL;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use PHPUnit\Framework\TestCase;

final class ETLErrorHandlingTest extends TestCase
{
    public function test_default_handler() : void
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
                        new DateEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
                        new NullEntry('phase')
                    )
                );

                yield new Rows(
                    Row::create(
                        new IntegerEntry('id', 102),
                        new BooleanEntry('deleted', true),
                        new DateEntry('expiration-date', new \DateTimeImmutable('2020-08-25')),
                        new NullEntry('phase')
                    )
                );
            }
        };

        $brokenTransformer = new class implements Transformer {
            public function transform(Rows $rows) : Rows
            {
                throw new \RuntimeException('Transformer Exception');
            }
        };

        $loader = new class implements Loader {
            public array $result = [];

            public function load(Rows $rows) : void
            {
                $this->result = \array_merge($this->result, $rows->toArray());
            }
        };

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Transformer Exception');

        ETL::extract($extractor)
            ->onError(new ThrowError())
            ->transform($brokenTransformer)
            ->load($loader)
            ->run();
    }

    public function test_ignore_error_handler() : void
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
                        new DateEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
                        new NullEntry('phase')
                    )
                );

                yield new Rows(
                    Row::create(
                        new IntegerEntry('id', 102),
                        new BooleanEntry('deleted', true),
                        new DateEntry('expiration-date', new \DateTimeImmutable('2020-08-25')),
                        new NullEntry('phase')
                    )
                );
            }
        };

        $brokenTransformer = new class implements Transformer {
            public function transform(Rows $rows) : Rows
            {
                throw new \RuntimeException('Transformer Exception');
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
            ->transform($brokenTransformer)
            ->load($loader)
            ->run();

        $this->assertEquals(
            [
                [
                    'id' => 101,
                    'deleted' => false,
                    'expiration-date' => '2020-08-24',
                    'phase' => null,
                ],
                [
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => '2020-08-25',
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
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract() : \Generator
            {
                yield new Rows(
                    Row::create(
                        new IntegerEntry('id', 101),
                        new BooleanEntry('deleted', false),
                        new DateEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
                        new NullEntry('phase')
                    )
                );

                yield new Rows(
                    Row::create(
                        new IntegerEntry('id', 102),
                        new BooleanEntry('deleted', true),
                        new DateEntry('expiration-date', new \DateTimeImmutable('2020-08-25')),
                        new NullEntry('phase')
                    )
                );
            }
        };

        $brokenTransformer = new class implements Transformer {
            public function transform(Rows $rows) : Rows
            {
                if ($rows->first()->valueOf('id') === 101) {
                    throw new \RuntimeException('Transformer Exception');
                }

                return $rows;
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
            ->onError(new SkipRows())
            ->transform($brokenTransformer)
            ->load($loader)
            ->run();

        $this->assertEquals(
            [
                [
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => '2020-08-25',
                    'phase' => null,
                ],
            ],
            $loader->result,
        );
    }
}
