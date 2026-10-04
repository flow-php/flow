<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\ParquetLoader;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Filesystem\SaveMode;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function implode;
use function sort;

final class ParquetLoaderTest extends TestCase
{
    public function test_a_second_run_writes_under_its_own_schema(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $loader = to_parquet(path('memory://runs/data.parquet'), filesystem: $memory)->saveMode(SaveMode::Append);

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context);
        $loader->closure($context);
        $loader->load(array_to_rows([['name' => 'a']], schema(str_schema('name'))), $context);
        $loader->closure($context);

        $written = [];

        foreach ($memory->list(path('memory://runs/*.parquet')) as $file) {
            $written[] = implode(',', ParquetFilesContext::columnNames($memory, $file->path->uri()));
        }

        sort($written);

        static::assertSame(['id', 'name'], $written);
    }

    public function test_setting_content_type_on_path(): void
    {
        $loader = new ParquetLoader(path(__DIR__ . '/file.parquet'));

        static::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::PARQUET);
    }
}
