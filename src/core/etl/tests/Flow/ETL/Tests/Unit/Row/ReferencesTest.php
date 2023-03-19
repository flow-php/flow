<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\References;
use PHPUnit\Framework\TestCase;
use function Flow\ETL\DSL\col;

final class ReferencesTest extends TestCase
{
    public function test_that_reference_without_alias_exists() : void
    {
        $refs = new References(col("id"), col("name"));

        $this->assertTrue($refs->has(col("id")));
        $this->assertFalse($refs->has(col("test")));
    }

    public function test_that_reference_with_alias_exists() : void
    {
        $refs = new References(col("id")->as("test"), col("name"));

        $this->assertFalse($refs->has(col("id")));
        $this->assertTrue($refs->has(col("test")));
    }
}