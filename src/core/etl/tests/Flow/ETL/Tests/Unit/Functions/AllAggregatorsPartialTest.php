<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Storage\Floe\FloeFiles;
use Flow\ETL\Tests\Context\AggregatorFixtures;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function range;
use function var_export;

final class AllAggregatorsPartialTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{class-string<AggregatingFunction>}>
     */
    public static function classes(): Generator
    {
        foreach (AggregatorFixtures::classes() as $class) {
            yield $class => [$class];
        }
    }

    /**
     * @return Generator<string, array{AggregatingFunction}>
     */
    public static function instances(): Generator
    {
        foreach (AggregatorFixtures::classes() as $class) {
            yield $class => [AggregatorFixtures::resolved($class)];
        }

        yield 'count without a reference' => [count()];
    }

    /**
     * @param class-string<AggregatingFunction> $class
     */
    #[DataProvider('classes')]
    public function test_partials_merged_in_order_equal_one_pass(string $class): void
    {
        $rows = AggregatorFixtures::rows(new PhpBackend());
        $context = flow_context();
        $scratch = ReaderRoundTrip::scratch('partials');
        $filesystem = native_local_filesystem();

        $onePass = AggregatorFixtures::resolved($class);
        $onePass->aggregate($rows, range(0, 5), $context->backend());

        $merged = AggregatorFixtures::resolved($class);

        foreach ([[0, 1], [2, 3], [4, 5]] as $part => $indices) {
            $aggregator = (new Aggregators(AggregatorFixtures::resolved($class)))->fresh()->first();
            $aggregator->aggregate($rows, $indices, $context->backend());

            $schema = schema(structure_schema('partial', $aggregator->partialType()));
            $path = $scratch->suffix('part-' . $part . '.floe');
            $writer = (new FloeFiles($filesystem, new PhpBackend()))->create($path, $schema);
            $writer->write(array_to_rows([['partial' => $aggregator->partial()]], $schema));
            $writer->close();

            foreach ((new FloeFiles($filesystem, new PhpBackend()))
                ->read($path)
                ->rows() as $batch) {
                $merged->merge(
                    type_map(type_string(), type_mixed())->assert($batch->column('partial')->value(0)),
                    $context->backend(),
                );
            }
        }

        $filesystem->rm($scratch);

        static::assertSame(var_export($onePass->value(), true), var_export($merged->value(), true));
    }

    #[DataProvider('instances')]
    public function test_a_malformed_partial_is_refused(AggregatingFunction $aggregator): void
    {
        $this->expectException(InvalidTypeException::class);

        $aggregator->merge(['not' => 'a partial'], flow_context()->backend());
    }

    #[DataProvider('instances')]
    public function test_partial_matches_partial_type(AggregatingFunction $aggregator): void
    {
        $aggregator->partialType()->assert($aggregator->partial());

        $aggregator->aggregate(AggregatorFixtures::rows(new PhpBackend()), range(0, 5), flow_context()->backend());

        $aggregator->partialType()->assert($aggregator->partial());

        static::assertTrue(true);
    }

    #[DataProvider('instances')]
    public function test_fresh_has_no_state(AggregatingFunction $aggregator): void
    {
        $untouched = $aggregator->withChildren($aggregator->children());
        $aggregator->aggregate(AggregatorFixtures::rows(new PhpBackend()), range(0, 5), flow_context()->backend());

        $fresh = (new Aggregators($aggregator))->fresh()->first();

        static::assertNotSame($aggregator, $fresh);
        static::assertSame(var_export($untouched->value(), true), var_export($fresh->value(), true));
        static::assertSame($untouched->partial(), $fresh->partial());
    }
}
