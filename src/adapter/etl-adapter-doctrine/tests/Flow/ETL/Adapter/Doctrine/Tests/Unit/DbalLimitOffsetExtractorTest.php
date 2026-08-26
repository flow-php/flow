<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\DriverManager;
use Flow\ETL\Adapter\Doctrine\DbalLimitOffsetExtractor;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class DbalLimitOffsetExtractorTest extends FlowTestCase
{
    public function test_schema_is_refused_when_it_was_not_declared(): void
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $extractor = new DbalLimitOffsetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
        );

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('cannot describe what it will produce before producing it');

        $extractor->schema();
    }

    public function test_schema_is_the_declared_one(): void
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $extractor = new DbalLimitOffsetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
        );

        static::assertEquals(schema(int_schema('id')), $extractor->withSchema(schema(int_schema('id')))->schema());
    }
}
