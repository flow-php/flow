<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use Closure;
use Flow\Types\Value\Json;
use ReflectionClass;

use function substr;
use function unpack;

final class JsonDecoder implements ValueDecoder
{
    /**
     * @var \Closure(string, bool): Json
     */
    private readonly Closure $create;

    public function __construct()
    {
        $reflection = new ReflectionClass(Json::class);

        /** @var \Closure(string, bool): Json $create */
        $create = Closure::bind(
            static function (string $value, bool $isObject) use ($reflection): Json {
                $json = $reflection->newInstanceWithoutConstructor();
                $json->value = $value;
                $json->isObject = $isObject;

                return $json;
            },
            null,
            Json::class,
        );

        $this->create = $create;
    }

    public function decode(string $data, int &$position): Json
    {
        $length = unpack('V', $data, $position)[1];
        $json = substr($data, $position + 4, $length);
        $position += 4 + $length;

        return ($this->create)($json, $data[$position++] === "\x01");
    }
}
