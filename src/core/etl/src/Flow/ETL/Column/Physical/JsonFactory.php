<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use Closure;
use Flow\Types\Value\Json;
use ReflectionClass;

/**
 * Builds a Json from text the column already validated - skips json_validate().
 */
final readonly class JsonFactory
{
    /**
     * @var \Closure(string): Json
     */
    private Closure $create;

    public function __construct()
    {
        $reflection = new ReflectionClass(Json::class);

        /** @var \Closure(string): Json $create */
        $create = Closure::bind(
            static function (string $value) use ($reflection): Json {
                $json = $reflection->newInstanceWithoutConstructor();
                $json->value = $value;
                $json->isObject = $value[0] === '{';

                return $json;
            },
            null,
            Json::class,
        );

        $this->create = $create;
    }

    public function create(string $json): Json
    {
        return ($this->create)($json);
    }
}
