<?php declare(strict_types=1);

namespace Flow\Dremel;

use Flow\Dremel\Exception\RuntimeException;

final class Stack
{
    /**
     * @var Node[]
     */
    private array $nodes = [];

    public function __construct()
    {
    }

    public function __debugInfo() : array
    {
        $output = [];

        foreach ($this->nodes as $node) {
            $output[] = $node->value();
        }

        return $output;
    }

    public function clear() : void
    {
        $this->nodes = [];
    }

    public function dropFlat() : ?array
    {
        $output = [];

        if (\count($this->nodes) === 1) {
            return $this->nodes[0]->value();
        }

        foreach ($this->nodes as $node) {
            $output[] = $node->value();
        }

        $this->nodes = [];

        return $output;
    }

    public function last() : Node
    {
        if (\count($this->nodes) === 0) {
            throw new RuntimeException('Stack is empty');
        }

        return $this->nodes[\count($this->nodes) - 1];
    }

    public function pop() : Node
    {
        if (\count($this->nodes) === 0) {
            throw new RuntimeException('Stack is empty');
        }

        return \array_pop($this->nodes);
    }

    public function push(Node $node) : void
    {
        $this->nodes[] = $node;
    }

    public function size() : int
    {
        return \count($this->nodes);
    }
}
