<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\ErrorHandler;

use DateTimeImmutable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Tests\Double\ThrowingAfterFirstBatchExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use RuntimeException;
use SensitiveParameter;

use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ignore_error_handler;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\skip_rows_handler;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\throw_error_handler;

final class ErrorHandlingTest extends FlowTestCase
{
    public function test_a_source_ended_by_the_handler_takes_the_chained_sources_after_it(): void
    {
        static::assertSame(
            [['id' => 1]],
            data_frame()
                ->read(from_all(new ThrowingAfterFirstBatchExtractor(), from_array([['id' => 2]])))
                ->onError(ignore_error_handler())
                ->fetch()
                ->toArray(),
        );
    }

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
             * @return \Generator<int, Rows, Signal|null, void>
             */
            public function extract(Backend $backend, Criteria $criteria): Generator
            {
                $schema = schema(
                    int_schema('id'),
                    bool_schema('deleted'),
                    datetime_schema('expiration-date'),
                    str_schema('phase', true),
                );

                yield array_to_rows([[
                    'id' => 101,
                    'deleted' => false,
                    'expiration-date' => new DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ]], $schema);

                yield array_to_rows([[
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ]], $schema);
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }

            public function isRepeatable(): bool
            {
                return false;
            }
        };

        $brokenTransformer = new class implements Transformer {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
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
                throw new RuntimeException('Transformer Exception');
            }
        };

        $loader = new class implements Loader {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
            }

            public function describe(): ?Description
            {
                return null;
            }

            /** @var array<array-key, mixed> */
            public array $result = [];

            public function load(Rows $rows, Backend $backend): void
            {
                $this->result = array_merge($this->result, $rows->toArray());
            }
        };

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Transformer Exception');

        data_frame()->read($extractor)->onError(throw_error_handler())->with($brokenTransformer)->write($loader)->run();
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
             * @return \Generator<int, Rows, Signal|null, void>
             */
            public function extract(Backend $backend, Criteria $criteria): Generator
            {
                $schema = schema(
                    int_schema('id'),
                    bool_schema('deleted'),
                    datetime_schema('expiration-date'),
                    str_schema('phase', true),
                );

                yield array_to_rows([[
                    'id' => 101,
                    'deleted' => false,
                    'expiration-date' => new DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ]], $schema);

                yield array_to_rows([[
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ]], $schema);
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }

            public function isRepeatable(): bool
            {
                return false;
            }
        };

        $brokenTransformer = new class implements Transformer {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
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
                throw new RuntimeException('Transformer Exception');
            }
        };

        $loader = new class implements Loader {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
            }

            public function describe(): ?Description
            {
                return null;
            }

            /** @var array<array-key, mixed> */
            public array $result = [];

            public function load(Rows $rows, Backend $backend): void
            {
                $this->result = array_merge($this->result, $rows->toArray());
            }
        };

        data_frame()
            ->read($extractor)
            ->onError(ignore_error_handler())
            ->with($brokenTransformer)
            ->write($loader)
            ->run();

        // a declined transformation drops its batch - running the remaining steps on it would load a half
        // transformed batch
        static::assertSame([], $loader->result);
    }

    public function test_skip_rows_handler_does_not_emit_the_half_transformed_batch(): void
    {
        $rows = data_frame()
            ->read(from_array(
                [['id' => 1, 'v' => '10'], ['id' => 2, 'v' => 'boom'], ['id' => 3, 'v' => '30']],
                schema(int_schema('id'), str_schema('v')),
            )->withBatchSize(1))
            ->onError(skip_rows_handler())
            ->withColumn('doubled', ref('v')->cast('integer')->multiply(lit(2)))
            ->fetch();

        static::assertSame(
            [
                ['id' => 1, 'v' => '10', 'doubled' => 20],
                ['id' => 3, 'v' => '30', 'doubled' => 60],
            ],
            $rows->toArray(),
        );
        static::assertSame(['id', 'v', 'doubled'], $rows->schema()->names());
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
             * @return \Generator<int, Rows, Signal|null, void>
             */
            public function extract(Backend $backend, Criteria $criteria): Generator
            {
                $schema = schema(
                    int_schema('id'),
                    bool_schema('deleted'),
                    datetime_schema('expiration-date'),
                    str_schema('phase', true),
                );

                yield array_to_rows([[
                    'id' => 101,
                    'deleted' => false,
                    'expiration-date' => new DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ]], $schema);

                yield array_to_rows([[
                    'id' => 102,
                    'deleted' => true,
                    'expiration-date' => new DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ]], $schema);
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }

            public function isRepeatable(): bool
            {
                return false;
            }
        };

        $brokenTransformer = new class implements Transformer {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
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
                if ($rows->column('id')->value(0) === 101) {
                    throw new RuntimeException('Transformer Exception');
                }

                return $rows;
            }
        };

        $loader = new class implements Loader {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
            }

            public function describe(): ?Description
            {
                return null;
            }

            /** @var array<array-key, mixed> */
            public array $result = [];

            public function load(Rows $rows, Backend $backend): void
            {
                $this->result = array_merge($this->result, $rows->toArray());
            }
        };

        data_frame()->read($extractor)->onError(skip_rows_handler())->with($brokenTransformer)->write($loader)->run();

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
