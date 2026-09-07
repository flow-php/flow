<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer;
use Generator;
use RuntimeException;

use function array_merge;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\ignore_error_handler;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\skip_rows_handler;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\throw_error_handler;

final class ETLErrorHandlingTest extends FlowTestCase
{
    public function test_default_handler(): void
    {
        $extractor = new class implements Extractor {
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
                $schema = schema(
                    int_schema('id'),
                    bool_schema('deleted'),
                    datetime_schema('expiration-date'),
                    str_schema('phase', true),
                );

                yield rows($schema, row([
                    'id' => 101,
                    'deleted' => false,
                    'expiration-date' => new DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ]));

                yield rows($schema, row([
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ]));
            }
        };

        $brokenTransformer = new class implements Transformer {
            public function bind(Schema $input): BoundStep
            {
                return new BoundStep($this, $input);
            }

            public function transform(Rows $rows, FlowContext $context): Rows
            {
                throw new RuntimeException('Transformer Exception');
            }
        };

        $loader = new class implements Loader {
            /** @var array<array-key, mixed> */
            public array $result = [];

            public function load(Rows $rows, FlowContext $context): void
            {
                $this->result = array_merge($this->result, $rows->toArray());
            }
        };

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Transformer Exception');

        data_frame()
            ->extract($extractor)
            ->onError(throw_error_handler())
            ->with($brokenTransformer)
            ->load($loader)
            ->run();
    }

    public function test_ignore_error_handler(): void
    {
        $extractor = new class implements Extractor {
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
                $schema = schema(
                    int_schema('id'),
                    bool_schema('deleted'),
                    datetime_schema('expiration-date'),
                    str_schema('phase', true),
                );

                yield rows($schema, row([
                    'id' => 101,
                    'deleted' => false,
                    'expiration-date' => new DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ]));

                yield rows($schema, row([
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ]));
            }
        };

        $brokenTransformer = new class implements Transformer {
            public function bind(Schema $input): BoundStep
            {
                return new BoundStep($this, $input);
            }

            public function transform(Rows $rows, FlowContext $context): Rows
            {
                throw new RuntimeException('Transformer Exception');
            }
        };

        $loader = new class implements Loader {
            /** @var array<array-key, mixed> */
            public array $result = [];

            public function load(Rows $rows, FlowContext $context): void
            {
                $this->result = array_merge($this->result, $rows->toArray());
            }
        };

        data_frame()
            ->extract($extractor)
            ->onError(ignore_error_handler())
            ->with($brokenTransformer)
            ->load($loader)
            ->run();

        static::assertEquals(
            [
                [
                    'id' => 101,
                    'deleted' => false,
                    'expiration-date' => new DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ],
                [
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ],
            ],
            $loader->result,
        );
    }

    public function test_skip_rows_handler(): void
    {
        $extractor = new class implements Extractor {
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
                $schema = schema(
                    int_schema('id'),
                    bool_schema('deleted'),
                    datetime_schema('expiration-date'),
                    str_schema('phase', true),
                );

                yield rows($schema, row([
                    'id' => 101,
                    'deleted' => false,
                    'expiration-date' => new DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ]));

                yield rows($schema, row([
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ]));
            }
        };

        $brokenTransformer = new class implements Transformer {
            public function bind(Schema $input): BoundStep
            {
                return new BoundStep($this, $input);
            }

            public function transform(Rows $rows, FlowContext $context): Rows
            {
                if ($rows->first()->get('id') === 101) {
                    throw new RuntimeException('Transformer Exception');
                }

                return $rows;
            }
        };

        $loader = new class implements Loader {
            /** @var array<array-key, mixed> */
            public array $result = [];

            public function load(Rows $rows, FlowContext $context): void
            {
                $this->result = array_merge($this->result, $rows->toArray());
            }
        };

        data_frame()->extract($extractor)->onError(skip_rows_handler())->with($brokenTransformer)->load($loader)->run();

        static::assertEquals(
            [
                [
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ],
            ],
            $loader->result,
        );
    }
}
