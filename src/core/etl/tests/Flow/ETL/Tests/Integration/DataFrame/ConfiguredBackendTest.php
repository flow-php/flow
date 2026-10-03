<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\RustBackend;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function extension_loaded;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ConfiguredBackendTest extends FlowTestCase
{
    public function test_php_built_rows_are_read_into_the_configured_backend(): void
    {
        $rows = data_frame(config_builder()->backend(new AdaptiveBackend()))
            ->read(from_rows(
                array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')), new PhpBackend()),
                array_to_rows([['id' => 3]], schema(int_schema('id')), new PhpBackend()),
            ))
            ->batchSize(3)
            ->fetch();

        static::assertSame([1, 2, 3], $rows->column('id')->values());
        static::assertSame(
            extension_loaded('flow_php') ? 'Flow\ETL\Column\RustColumn' : 'Flow\ETL\Column\Php\ScalarColumn',
            $rows->column('id')::class,
        );
    }

    public function test_a_custom_extractor_yielding_php_rows_is_read_into_the_configured_backend(): void
    {
        $rows = data_frame(config_builder()->backend(new AdaptiveBackend()))
            ->read(
                new CountingExtractor(
                    schema(int_schema('id')),
                    array_to_rows([['id' => 1]], schema(int_schema('id')), new PhpBackend()),
                ),
            )
            ->fetch();

        static::assertSame(
            extension_loaded('flow_php') ? 'Flow\ETL\Column\RustColumn' : 'Flow\ETL\Column\Php\ScalarColumn',
            $rows->column('id')::class,
        );
    }

    #[RequiresPhpExtension('flow_php')]
    public function test_native_rows_are_read_into_php_columns_under_a_php_backend(): void
    {
        $rows = data_frame(config_builder()->backend(new PhpBackend()))
            ->read(from_rows(array_to_rows([['id' => 1]], schema(int_schema('id')), new RustBackend())))
            ->fetch();

        static::assertSame([1], $rows->column('id')->values());
        static::assertSame('Flow\ETL\Column\Php\ScalarColumn', $rows->column('id')::class);
    }
}
