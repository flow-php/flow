<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Schema;
use Flow\Floe\AdaptiveFloeEncoder;
use Flow\Floe\FloeEngine;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_entry;

final class FloeEngineTest extends TestCase
{
    public function test_adaptive_engine_builds_adaptive_encoder(): void
    {
        static::assertInstanceOf(AdaptiveFloeEncoder::class, FloeEngine::adaptive->encoder($this->schema()));
    }

    public function test_native_engine_builds_native_encoder_when_supported(): void
    {
        if (!NativeFloeEncoder::isSupported()) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        static::assertInstanceOf(NativeFloeEncoder::class, FloeEngine::native->encoder($this->schema()));
    }

    public function test_php_engine_builds_php_encoder(): void
    {
        static::assertInstanceOf(PhpFloeEncoder::class, FloeEngine::php->encoder($this->schema()));
    }

    public function test_case_values_are_stable_identifiers(): void
    {
        static::assertSame('adaptive', FloeEngine::adaptive->value);
        static::assertSame('native', FloeEngine::native->value);
        static::assertSame('php', FloeEngine::php->value);
    }

    private function schema(): Schema
    {
        $row = row(int_entry('id', 1), str_entry('name', 'flow'));

        return schema_from_json(FloeSchemaContext::schemaBody($row->schema()));
    }
}
