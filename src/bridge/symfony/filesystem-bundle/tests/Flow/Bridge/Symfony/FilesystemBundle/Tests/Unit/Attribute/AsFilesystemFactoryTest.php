<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Attribute;

use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystemFactory;
use PHPUnit\Framework\TestCase;

final class AsFilesystemFactoryTest extends TestCase
{
    public function test_attribute_targets_class_only() : void
    {
        $reflection = new \ReflectionClass(AsFilesystemFactory::class);
        $attributes = $reflection->getAttributes(\Attribute::class);

        self::assertCount(1, $attributes);
        self::assertSame(\Attribute::TARGET_CLASS, $attributes[0]->newInstance()->flags);
    }

    public function test_exposes_protocol_as_public_readonly() : void
    {
        $attribute = new AsFilesystemFactory('my-fs');

        self::assertSame('my-fs', $attribute->protocol);
    }
}
