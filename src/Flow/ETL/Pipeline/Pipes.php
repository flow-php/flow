<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{pipes: array<int, Pipe>}>
 */
final class Pipes implements Serializable
{
    /**
     * @var array<int, Pipe>
     */
    private array $pipes;

    /**
     * @param array<int, Pipe> $pipes
     */
    public function __construct(array $pipes)
    {
        $this->pipes = $pipes;
    }

    public static function empty() : self
    {
        return new self([]);
    }

    public function __serialize() : array
    {
        return [
            'pipes' => $this->pipes,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->pipes = $data['pipes'];
    }

    public function add(Pipe $pipe) : void
    {
        $this->pipes[] = $pipe;
    }

    /**
     * @return array<Pipe>
     */
    public function all() : array
    {
        return $this->pipes;
    }
}
