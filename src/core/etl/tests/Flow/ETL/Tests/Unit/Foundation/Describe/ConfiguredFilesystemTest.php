<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Describe;

use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\Described;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\MemoryFilesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;

final class ConfiguredFilesystemTest extends FlowTestCase
{
    public function test_a_configurable_filesystem_round_trips_as_a_secret(): void
    {
        $description = (new ConfiguredFilesystem(new NativeLocalFilesystem()))->describe();

        static::assertNotNull($description);
        static::assertTrue($description->sensitive());
        static::assertSame(
            ['filesystem' => NativeLocalFilesystem::class, 'configuration' => ['protocol' => 'file']],
            $description->data,
        );
        static::assertEquals(
            new NativeLocalFilesystem(),
            (new Rebuild())->of($description->normalize(), ConfiguredFilesystem::class)->filesystem(),
        );
    }

    public function test_a_filesystem_with_live_state_does_not_describe(): void
    {
        static::assertNull((new ConfiguredFilesystem(new MemoryFilesystem()))->describe());
    }

    public function test_a_class_that_is_not_a_filesystem_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Flow\ETL\Tests\Double\Described is not a concrete Filesystem');

        ConfiguredFilesystem::fromDescription(['filesystem' => Described::class, 'configuration' => []], new Rebuild());
    }
}
