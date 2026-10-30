<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage\Serializer;

use Flow\ETL\Storage\Serializer\Serializer;
use Flow\ETL\Tests\Context\DescribableFixtures;
use Flow\ETL\Tests\Context\ExtractorClasses;
use Flow\ETL\Tests\Context\ScalarFunctionClasses;
use Flow\ETL\Values\Column\PhpBackend;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;
use function Flow\Types\DSL\type_instance_of;

final class AllSerializersKeepTheBackendTest extends TestCase
{
    /**
     * @return Generator<class-string, array{class-string}>
     */
    public static function serializers(): Generator
    {
        foreach (ScalarFunctionClasses::implementing(Serializer::class, ExtractorClasses::root('core/etl')) as $class) {
            yield $class => [$class];
        }
    }

    /**
     * @param class-string $class
     */
    #[DataProvider('serializers')]
    public function test_a_batch_decodes_in_the_backend_it_was_built_in(string $class): void
    {
        $serializer = type_instance_of(Serializer::class)->assert(DescribableFixtures::all()[$class]());
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'one'], ['id' => 2, 'name' => 'two']],
            schema(int_schema('id'), str_schema('name')),
            new PhpBackend(),
        );

        $decoded = unserialize_from_string($serializer, serialize_to_string($serializer, $rows));

        static::assertSame($rows->column('id')::class, $decoded->column('id')::class);
        static::assertSame($rows->column('name')::class, $decoded->column('name')::class);
        static::assertSame([[1, 'one'], [2, 'two']], [
            [$decoded->column('id')->value(0), $decoded->column('name')->value(0)],
            [$decoded->column('id')->value(1), $decoded->column('name')->value(1)],
        ]);
    }
}
