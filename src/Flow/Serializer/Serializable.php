<?php declare(strict_types=1);

namespace Flow\Serializer;

interface Serializable
{
    /**
     * @return array<string, mixed>
     */
    public function __serialize() : array;

    /**
     * While not used by NativePHPSerializer it first requires Serializable to be instantiate
     * through Reflection without invoking constructor so __unserialize can bring object to the right state.
     *
     * @param array<string, mixed> $data
     */
    public function __unserialize(array $data) : void;
}
