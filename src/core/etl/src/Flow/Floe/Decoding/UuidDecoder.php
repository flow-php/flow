<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use Closure;
use Flow\Types\Value\Uuid;
use ReflectionClass;

use function substr;

final class UuidDecoder implements ValueDecoder
{
    /**
     * @var \Closure(string): Uuid
     */
    private readonly Closure $create;

    public function __construct()
    {
        $reflection = new ReflectionClass(Uuid::class);

        /** @var \Closure(string): Uuid $create */
        $create = Closure::bind(
            static function (string $value) use ($reflection): Uuid {
                $uuid = $reflection->newInstanceWithoutConstructor();
                // @mago-ignore analysis:invalid-property-write
                $uuid->value = $value;

                return $uuid;
            },
            null,
            Uuid::class,
        );

        $this->create = $create;
    }

    public function decode(string $data, int &$position): Uuid
    {
        $value = ($this->create)(substr($data, $position, 36));
        $position += 36;

        return $value;
    }
}
