<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Tests\Context\FloeFileContext;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\Floe\DSL\merge_floe;

final class FloeMergeDSLTest extends FlowIntegrationTestCase
{
    public function test_merge_floe_compact_over_path_objects(): void
    {
        $a = $this->cacheDir->suffix('mc-a.floe');
        $b = $this->cacheDir->suffix('mc-b.floe');
        $out = $this->cacheDir->suffix('mc-out.floe');

        FloeFileContext::write($this->fs(), $a, rows(row(int_entry('id', 1))));
        FloeFileContext::write($this->fs(), $b, rows(row(int_entry('id', 2))));

        merge_floe([$a, $b], $out, compact: true);

        static::assertEquals(
            rows(row(int_entry('id', 1)), row(int_entry('id', 2))),
            FloeFileContext::readAll($this->fs(), $out),
        );
    }

    public function test_merge_floe_splices_local_files_from_string_paths(): void
    {
        $a = $this->cacheDir->suffix('ms-a.floe');
        $b = $this->cacheDir->suffix('ms-b.floe');
        $out = $this->cacheDir->suffix('ms-out.floe');

        FloeFileContext::write($this->fs(), $a, rows(row(int_entry('id', 1))));
        FloeFileContext::write($this->fs(), $b, rows(row(int_entry('id', 2))));

        merge_floe([$a->path(), $b->path()], $out->path());

        static::assertEquals(
            rows(row(int_entry('id', 1)), row(int_entry('id', 2))),
            FloeFileContext::readAll($this->fs(), $out),
        );
    }
}
