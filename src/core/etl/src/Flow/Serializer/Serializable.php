<?php declare(strict_types=1);

namespace Flow\Serializer;

/**
 * @template T
 */
interface Serializable
{
    /**
     * @return T
     *
     * @phpstan-ignore-next-line
     */
    public function __serialize() : array;

    /**
     * While not used by NativePHPSerializer it first requires Serializable to be instantiate
     * through Reflection without invoking constructor so __unserialize can bring object to the right state.
     *
     * @param T $data
     *
     * @phpstan-ignore-next-line
     */
    public function __unserialize(array $data) : void;
}
