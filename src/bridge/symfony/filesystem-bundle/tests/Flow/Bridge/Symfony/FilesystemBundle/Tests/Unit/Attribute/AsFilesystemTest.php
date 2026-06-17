<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Attribute;

use Attribute;
use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystem;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class AsFilesystemTest extends TestCase
{
    public function test_attribute_targets_parameter_only(): void
    {
        $reflection = new ReflectionClass(AsFilesystem::class);
        $attributes = $reflection->getAttributes(Attribute::class);

        static::assertCount(1, $attributes);
        static::assertSame(Attribute::TARGET_PARAMETER, $attributes[0]->newInstance()->flags);
    }

    public function test_defaults_fstab_to_null(): void
    {
        static::assertNull((new AsFilesystem(mount: 'memory'))->fstab);
    }

    public function test_exposes_mount_and_fstab_as_public_readonly(): void
    {
        $attribute = new AsFilesystem(mount: 'file', fstab: 'archive');

        static::assertSame('file', $attribute->mount);
        static::assertSame('archive', $attribute->fstab);
    }
}
