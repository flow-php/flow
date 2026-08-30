<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

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
                return new Schema();
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
                return new Schema();
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
}
