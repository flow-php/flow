<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Loader;
use Flow\ETL\Transformer;

final class Pipes
{
    /**
     * @param array<int, Loader|Transformer> $pipes
     */
    public function __construct(private array $pipes)
    {
    }

    public static function empty() : self
    {
        return new self([]);
    }

    public function add(Loader|Transformer $pipe) : void
    {
        $this->pipes[] = $pipe;
    }

    /**
     * @return array<Loader|Transformer>
     */
    public function all() : array
    {
        return $this->pipes;
    }

    public function has(string $transformerClass) : bool
    {
        if (!\class_exists($transformerClass)) {
            return false;
        }

        if (!\is_subclass_of($transformerClass, Transformer::class)) {
            return false;
        }

        foreach ($this->pipes as $pipe) {
            if ($pipe instanceof $transformerClass) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return array<Loader>
     */
    public function loaders() : array
    {
        $loaders = [];

        foreach ($this->pipes as $pipe) {
            if ($pipe instanceof Loader) {
                $loaders[] = $pipe;
            }
        }

        return $loaders;
    }

    public function merge(self $pipes) : self
    {
        if (!\count($this->pipes)) {
            return $pipes;
        }

        if (!\count($pipes->pipes)) {
            return $this;
        }

        return new self(\array_merge($this->pipes, $pipes->pipes));
    }
}
