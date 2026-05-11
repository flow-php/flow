<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Operations;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Operations\OperationOptions;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\operation_options;

final class OperationOptionsTest extends TestCase
{
    public function test_custom_chunk_size_is_accepted(): void
    {
        static::assertSame(65536, operation_options(65536)->chunkSize);
    }

    public function test_default_chunk_size_is_8192(): void
    {
        static::assertSame(8192, operation_options()->chunkSize);
    }

    public function test_dsl_returns_operation_options_instance(): void
    {
        static::assertInstanceOf(OperationOptions::class, operation_options());
    }

    public function test_negative_chunk_size_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk size must be greater than zero, got -1.');

        operation_options(-1);
    }

    public function test_zero_chunk_size_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Chunk size must be greater than zero, got 0.');

        operation_options(0);
    }
}
