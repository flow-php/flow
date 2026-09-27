<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger\Tests\Unit;

use Flow\ETL\Adapter\Logger\PsrLoggerLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Psr\Log\LogLevel;
use Psr\Log\Test\TestLogger;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;

final class PsrLoggerLoaderTest extends FlowTestCase
{
    public function test_psr_logger_loader(): void
    {
        $logger = new TestLogger();

        $loader = new PsrLoggerLoader($logger, 'row log', LogLevel::ERROR);

        $loader->load(
            array_to_rows([['id' => 12345, 'name' => 'Norbert']], schema(int_schema('id'), string_schema('name'))),
            flow_context(config()),
        );

        static::assertTrue($logger->hasErrorRecords());
        static::assertTrue($logger->hasError('row log'));
    }

    public function test_a_json_column_is_logged_as_an_array(): void
    {
        $logger = new TestLogger();

        (new PsrLoggerLoader($logger, 'row log'))->load(array_to_rows([['payload' => Json::fromArray([
            'a' => 1,
        ])]], schema(json_schema('payload'))), flow_context(config()));

        static::assertSame(['payload' => ['a' => 1]], $logger->records[0]['context']);
    }
}
