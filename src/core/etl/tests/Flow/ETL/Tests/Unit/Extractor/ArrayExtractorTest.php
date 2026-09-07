<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use ArrayIterator;
use ArrayObject;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Tests\Double\FixedTmpDirFilesystem;
use Flow\ETL\Tests\Double\FreshRowsAggregate;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Types\Type\Native\UnionType;
use Generator;
use NoRewindIterator;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\execution_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function iterator_to_array;

final class ArrayExtractorTest extends FlowTestCase
{
    public function test_a_declared_schema_writes_no_spill(): void
    {
        /** @var callable(): Generator<int, array<string, mixed>> $generator */
        $generator = static function (): Generator {
            yield ['id' => 1];
            yield ['id' => 2];
        };

        $filesystem = memory_filesystem();
        $extractor = from_array(
            $generator(),
            filesystem: $filesystem,
            spillRoot: path('memory://tmp'),
        )->withSchema(schema(int_schema('id')));

        $rows = iterator_to_array($extractor->extract(execution_context(config())));

        static::assertCount(2, $rows);
        static::assertNull($filesystem->status(path('memory://tmp/flow-php-source/*.b64')));
    }

    public function test_a_generator_yields_the_exact_schema_of_a_row_past_any_sample_budget(): void
    {
        /** @var callable(): Generator<int, array<string, mixed>> $generator */
        $generator = static function (): Generator {
            for ($i = 0; $i < 25; $i++) {
                yield ['code' => $i];
            }

            yield ['code' => 'AB-01'];
        };

        static::assertSame(
            'string',
            from_array($generator(), filesystem: memory_filesystem(), spillRoot: path('memory://tmp'))
                ->inferSchema(infer_schema()->sampleSize(3))
                ->schema()
                ->get('code')
                ->type()
                ->toString(),
        );
    }

    public function test_a_one_shot_source_is_extracted_as_many_times_as_an_array_one(): void
    {
        /** @var callable(): Generator<int, array<string, mixed>> $generator */
        $generator = static function (): Generator {
            yield ['id' => 1];
            yield ['id' => 2];
        };

        $extractor = from_array($generator(), filesystem: memory_filesystem(), spillRoot: path('memory://tmp'));

        // The generator is advanced once; every later extract() replays the spill it produced, so
        // ->schema() followed by ->run(), or two run() calls, behave as they do for an array.
        static::assertCount(2, iterator_to_array($extractor->extract(execution_context(config()))));
        static::assertCount(2, iterator_to_array($extractor->extract(execution_context(config()))));
    }

    public function test_infer_schema_resets_the_memo(): void
    {
        $extractor = from_array([['code' => 1000], ['code' => 1001], ['code' => 'AB-01']]);

        static::assertSame('string', $extractor->schema()->get('code')->type()->toString());

        $extractor->inferSchema(infer_schema()->sampleSize(2));

        static::assertSame('integer', $extractor->schema()->get('code')->type()->toString());
    }

    public function test_infer_schema_after_a_one_shot_source_was_consumed_is_refused(): void
    {
        /** @var callable(): Generator<int, array<string, mixed>> $generator */
        $generator = static function (): Generator {
            yield ['id' => 1];
        };

        $extractor = from_array($generator(), filesystem: memory_filesystem(), spillRoot: path('memory://tmp'));
        $extractor->schema();

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('cannot change its inference options');

        $extractor->inferSchema(infer_schema()->allStrings());
    }

    /**
     * Every iterable shape - rewindable or not - is described exactly and yields its rows once. There is
     * no rewindability predicate: is_array() takes the free in-place double read and everything else
     * takes the spill, which is correct for a rewindable ArrayIterator too. It merely pays the spill for
     * a source it could have re-read; guessing the other way loses rows silently.
     */
    #[TestWith(['array'])]
    #[TestWith(['ArrayIterator'])]
    #[TestWith(['ArrayObject'])]
    #[TestWith(['IteratorAggregate'])]
    #[TestWith(['Generator'])]
    #[TestWith(['NoRewindIterator'])]
    public function test_every_iterable_shape_yields_the_same_schema_and_rows(string $shape): void
    {
        $rows = [['id' => 1, 'name' => 'Norbert'], ['id' => 2, 'name' => 'Michal']];

        /** @var callable(): Generator<int, array<string, mixed>> $rowsGenerator */
        $rowsGenerator = static function () use ($rows): Generator {
            yield from $rows;
        };

        /** @var iterable<array<mixed>> $dataset */
        $dataset = match ($shape) {
            'array' => $rows,
            'ArrayIterator' => new ArrayIterator($rows),
            'ArrayObject' => new ArrayObject($rows),
            'IteratorAggregate' => new FreshRowsAggregate($rows),
            'Generator' => $rowsGenerator(),
            default => new NoRewindIterator(new ArrayIterator($rows)),
        };

        $extractor = from_array($dataset, filesystem: memory_filesystem(), spillRoot: path('memory://tmp'));

        static::assertSame(
            ['id' => 'integer', 'name' => 'string'],
            array_map(static fn(Definition $definition): string => $definition
                ->type()
                ->toString(), $extractor->schema()->definitions()),
        );
        static::assertSame(
            [['id' => 1, 'name' => 'Norbert'], ['id' => 2, 'name' => 'Michal']],
            array_map(static fn(Rows $batch): array => $batch
                ->first()
                ->toArray(), iterator_to_array($extractor->extract(execution_context(config())))),
        );
    }

    public function test_array_extractor(): void
    {
        $extractor = from_array([
            ['id' => 1, 'name' => 'Norbert'],
            ['id' => 2, 'name' => 'Michal'],
        ]);

        $rows = iterator_to_array($extractor->extract(execution_context(config_builder()->build())));

        static::assertCount(2, $rows);
        static::assertInstanceOf(Rows::class, $rows[0]);
        static::assertInstanceOf(Rows::class, $rows[1]);
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $rows[0]->first()->toArray());
        static::assertSame(['id' => 2, 'name' => 'Michal'], $rows[1]->first()->toArray());
    }

    public function test_extraction_with_a_union_column_in_the_schema(): void
    {
        /** @var UnionType<mixed, mixed> $union */
        $union = type_union(type_string(), type_integer());

        $extractor = from_array(
            [
                ['id' => 1, 'a' => 42],
                ['id' => 2, 'a' => 'x'],
                ['id' => 3, 'a' => null],
            ],
            schema: schema(int_schema('id'), new UnionDefinition('a', $union, true)),
        );

        $rows = iterator_to_array($extractor->extract(execution_context(config_builder()->build())));

        static::assertSame(42, $rows[0]->first()->get('a'));
        static::assertSame('x', $rows[1]->first()->get('a'));
        static::assertNull($rows[2]->first()->get('a'));
        static::assertInstanceOf(UnionDefinition::class, $rows[0]->schema()->get('a'));
    }

    public function test_generator_extraction_with_a_declared_schema(): void
    {
        /** @var callable(): Generator<int, array<string, mixed>> $generator */
        $generator = static function (): Generator {
            yield ['id' => 1, 'name' => 'Norbert'];
            yield ['id' => 2, 'name' => 'Michal'];
        };

        $extractor = from_array($generator())->withSchema(schema(int_schema('id'), str_schema('name')));

        $rows = iterator_to_array($extractor->extract(execution_context(config())));

        static::assertCount(2, $rows);
        static::assertInstanceOf(Rows::class, $rows[0]);
        static::assertInstanceOf(Rows::class, $rows[1]);
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $rows[0]->first()->toArray());
        static::assertSame(['id' => 2, 'name' => 'Michal'], $rows[1]->first()->toArray());
    }

    public function test_a_retry_after_the_source_broke_is_refused_rather_than_re_read(): void
    {
        /** @var callable(): Generator<int, array<string, mixed>> $generator */
        $generator = static function (): Generator {
            yield ['id' => 1];

            throw new FilesystemRuntimeException('connection reset by peer');
        };

        $extractor = from_array($generator(), filesystem: memory_filesystem(), spillRoot: path('memory://tmp'));

        try {
            $extractor->schema();
        } catch (FilesystemRuntimeException) {
        }

        // Re-reading the half-consumed source would answer with 0 columns and extract 0 rows, silently,
        // for every shape whose exhaustion is quiet.
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('The dataset was already consumed');

        $extractor->schema();
    }

    public function test_a_generator_with_no_spill_root_spills_under_the_system_tmp_dir(): void
    {
        /** @var callable(): Generator<int, array<string, mixed>> $generator */
        $generator = static function (): Generator {
            yield ['id' => 1];
            yield ['id' => 2];
        };

        $filesystem = new FixedTmpDirFilesystem(memory_filesystem(), path('memory://systmp'));
        $extractor = from_array($generator(), filesystem: $filesystem);

        $extractor->schema();

        static::assertNotNull($filesystem->status(path('memory://systmp/flow-php-source/*.b64')));
        static::assertCount(2, iterator_to_array($extractor->extract(execution_context(config()))));

        unset($extractor);

        static::assertNull($filesystem->status(path('memory://systmp/flow-php-source/*.b64')));
    }

    public function test_a_generator_is_described_exactly_and_extracted_once(): void
    {
        /** @var callable(): Generator<int, array<string, mixed>> $generator */
        $generator = static function (): Generator {
            yield ['id' => 1];
            yield ['id' => 2];
            yield ['id' => 'AB-01'];
        };

        $extractor = from_array($generator(), filesystem: memory_filesystem(), spillRoot: path('memory://tmp'));

        static::assertSame('string', $extractor->schema()->get('id')->type()->toString());

        $rows = iterator_to_array($extractor->extract(execution_context(config())));

        static::assertCount(3, $rows);
        static::assertSame([['id' => '1'], ['id' => '2'], ['id' => 'AB-01']], [
            $rows[0]->first()->toArray(),
            $rows[1]->first()->toArray(),
            $rows[2]->first()->toArray(),
        ]);
    }

    /**
     * An int-keyed row is a positional record. The derived schema must name its columns exactly as
     * array_to_rows() does, or the schema names never match the entries.
     *
     * @param list<array<mixed>> $dataset
     * @param list<non-empty-string> $expected
     */
    #[TestWith([[['a', 'b']], ['e00', 'e01']])]
    #[TestWith([[[10 => 'x', 20 => 'y']], ['e10', 'e20']])]
    #[TestWith([[['0' => 'z']], ['e00']])]
    #[TestWith([[['id' => 1, 'name' => 'n']], ['id', 'name']])]
    public function test_positional_rows_are_named_like_the_hydrator_names_them(array $dataset, array $expected): void
    {
        static::assertSame($expected, array_keys(from_array($dataset)->schema()->definitions()));
        static::assertSame(
            $expected,
            array_keys(data_frame()->read(from_array($dataset))->fetch()->first()->toArray()),
        );
    }

    public function test_a_bounded_sample_is_opt_in(): void
    {
        $dataset = [['code' => 1000], ['code' => 1001], ['code' => 1002], ['code' => 'AB-01']];

        static::assertSame(
            'integer',
            from_array($dataset)->inferSchema(infer_schema()->sampleSize(3))->schema()->get('code')->type()->toString(),
        );
        static::assertSame('string', from_array($dataset)->schema()->get('code')->type()->toString());

        $this->expectException(SchemaMismatchException::class);

        iterator_to_array(
            from_array($dataset)
                ->inferSchema(infer_schema()->sampleSize(3))
                ->extract(execution_context(config_builder()->hydrator(new PhpRowHydrator())->build())),
        );
    }

    public function test_a_column_holding_only_empty_arrays_floors_to_json(): void
    {
        static::assertSame(
            'json',
            data_frame()
                ->read(from_array([['tags' => []]]))
                ->schema()
                ->findDefinition('tags')
                ?->type()
                ->toString(),
        );
    }

    public function test_schema_is_memoised(): void
    {
        $extractor = from_array([['id' => 1]]);

        static::assertSame($extractor->schema(), $extractor->schema());
    }

    public function test_an_empty_array_does_not_downgrade_the_column_to_json(): void
    {
        static::assertSame(
            'list<?string>',
            data_frame()
                ->read(from_array([['tags' => ['a', 'b']], ['tags' => []]]))
                ->schema()
                ->findDefinition('tags')
                ?->type()
                ->toString(),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_array([['id' => 1]])->isRepeatable());
    }
}
