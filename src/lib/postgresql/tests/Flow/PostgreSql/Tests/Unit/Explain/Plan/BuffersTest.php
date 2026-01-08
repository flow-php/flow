<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain\Plan;

use Flow\PostgreSql\Explain\Plan\Buffers;
use PHPUnit\Framework\TestCase;

final class BuffersTest extends TestCase
{
    public function test_from_array_and_normalize_are_inverse() : void
    {
        $original = new Buffers(
            sharedHit: 100,
            sharedRead: 50,
            sharedDirtied: 10,
            sharedWritten: 5,
            localHit: 20,
            localRead: 10,
            localDirtied: 2,
            localWritten: 1,
            tempRead: 15,
            tempWritten: 8,
        );

        $normalized = $original->normalize();
        $restored = Buffers::fromArray($normalized);

        self::assertEquals($original, $restored);
    }

    public function test_from_array_creates_instance() : void
    {
        $data = [
            'shared_hit' => 100,
            'shared_read' => 50,
            'shared_dirtied' => 10,
            'shared_written' => 5,
            'local_hit' => 20,
            'local_read' => 10,
            'local_dirtied' => 2,
            'local_written' => 1,
            'temp_read' => 15,
            'temp_written' => 8,
        ];

        $buffers = Buffers::fromArray($data);

        self::assertSame(100, $buffers->sharedHit());
        self::assertSame(50, $buffers->sharedRead());
        self::assertSame(10, $buffers->sharedDirtied());
        self::assertSame(5, $buffers->sharedWritten());
        self::assertSame(20, $buffers->localHit());
        self::assertSame(10, $buffers->localRead());
        self::assertSame(2, $buffers->localDirtied());
        self::assertSame(1, $buffers->localWritten());
        self::assertSame(15, $buffers->tempRead());
        self::assertSame(8, $buffers->tempWritten());
    }

    public function test_normalize_returns_all_fields() : void
    {
        $buffers = new Buffers(
            sharedHit: 100,
            sharedRead: 50,
            sharedDirtied: 10,
            sharedWritten: 5,
            localHit: 20,
            localRead: 10,
            localDirtied: 2,
            localWritten: 1,
            tempRead: 15,
            tempWritten: 8,
        );

        $normalized = $buffers->normalize();

        self::assertSame(100, $normalized['shared_hit']);
        self::assertSame(50, $normalized['shared_read']);
        self::assertSame(10, $normalized['shared_dirtied']);
        self::assertSame(5, $normalized['shared_written']);
        self::assertSame(20, $normalized['local_hit']);
        self::assertSame(10, $normalized['local_read']);
        self::assertSame(2, $normalized['local_dirtied']);
        self::assertSame(1, $normalized['local_written']);
        self::assertSame(15, $normalized['temp_read']);
        self::assertSame(8, $normalized['temp_written']);
    }

    public function test_normalize_returns_expected_keys() : void
    {
        $buffers = new Buffers(
            sharedHit: 0,
            sharedRead: 0,
            sharedDirtied: 0,
            sharedWritten: 0,
            localHit: 0,
            localRead: 0,
            localDirtied: 0,
            localWritten: 0,
            tempRead: 0,
            tempWritten: 0,
        );

        $normalized = $buffers->normalize();

        $expectedKeys = [
            'shared_hit',
            'shared_read',
            'shared_dirtied',
            'shared_written',
            'local_hit',
            'local_read',
            'local_dirtied',
            'local_written',
            'temp_read',
            'temp_written',
        ];

        self::assertSame($expectedKeys, \array_keys($normalized));
    }
}
