<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Floe\DSL\merge_floe;

final class FloeMergeDSLTest extends FlowIntegrationTestCase
{
    public function test_merge_floe_compact_over_path_objects(): void
    {
        $a = $this->cacheDir->suffix('mc-a.floe');
        $b = $this->cacheDir->suffix('mc-b.floe');
        $out = $this->cacheDir->suffix('mc-out.floe');

        FloeStreamReaderContext::write($this->fs(), $a, array_to_rows([['id' => 1]], schema(int_schema('id'))));
        FloeStreamReaderContext::write($this->fs(), $b, array_to_rows([['id' => 2]], schema(int_schema('id'))));

        merge_floe([$a, $b], $out, new AdaptiveBackend(), compact: true);

        static::assertEquals(
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
            FloeStreamReaderContext::readAll($this->fs(), $out),
        );
    }

    public function test_merge_floe_splices_local_files_from_string_paths(): void
    {
        $a = $this->cacheDir->suffix('ms-a.floe');
        $b = $this->cacheDir->suffix('ms-b.floe');
        $out = $this->cacheDir->suffix('ms-out.floe');

        FloeStreamReaderContext::write($this->fs(), $a, array_to_rows([['id' => 1]], schema(int_schema('id'))));
        FloeStreamReaderContext::write($this->fs(), $b, array_to_rows([['id' => 2]], schema(int_schema('id'))));

        merge_floe([$a->path(), $b->path()], $out->path(), new AdaptiveBackend());

        static::assertEquals(
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
            FloeStreamReaderContext::readAll($this->fs(), $out),
        );
    }
}
