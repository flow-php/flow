<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Resource;

final class ResourceMother
{
    public static function default() : Resource
    {
        return Resource::create([
            'service.name' => 'test-service',
            'service.version' => '1.0.0',
        ]);
    }

    /**
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes
     */
    public static function with(array $attributes) : Resource
    {
        return Resource::create($attributes);
    }
}
