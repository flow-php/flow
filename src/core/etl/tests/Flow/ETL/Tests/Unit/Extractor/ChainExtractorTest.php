<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class ChainExtractorTest extends FlowTestCase
{
    public function test_chain_extractor(): void
    {
        $extractor = from_all(new class implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return schema(int_schema('id'));
            }

            public function extract(FlowContext $context): Generator
            {
                yield rows(schema(int_schema('id')), row(['id' => 1]));
                yield rows(schema(int_schema('id')), row(['id' => 2]));
            }
        }, new class implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return schema(int_schema('id'));
            }

            public function extract(FlowContext $context): Generator
            {
                yield rows(schema(int_schema('id')), row(['id' => 3]));
                yield rows(schema(int_schema('id')), row(['id' => 4]));
            }
        });

        self::assertExtractedRowsEquals(
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]), row(['id' => 4])),
            $extractor,
        );
    }

    public function test_with_schema_does_not_leak_into_a_second_pipeline(): void
    {
        $child = from_rows(rows(schema(int_schema('id')), row(['id' => 1])));

        iterator_to_array(
            from_all($child)
                ->withSchema(schema(int_schema('id'), str_schema('name', nullable: true)))
                ->extract(flow_context()),
            false,
        );

        static::assertTrue($child->schema()->isSame(schema(int_schema('id'))));
        static::assertSame(
            [['id' => 1]],
            iterator_to_array(from_all($child)->extract(flow_context()), false)[0]->toArray(),
        );
    }
}
