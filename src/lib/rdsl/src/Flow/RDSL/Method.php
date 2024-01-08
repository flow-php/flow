<?php declare(strict_types=1);

namespace Flow\RDSL;

final class Method implements Executable
{
    private ?Method $call = null;

    public function __construct(
        private readonly string $name,
        private readonly Arguments $arguments,
    ) {
    }

    public function addMethodCall(self $executable) : self
    {
        $this->call = $executable;

        return $this;
    }

    public function arguments() : Arguments
    {
        return $this->arguments;
    }

    public function call() : ?self
    {
        return $this->call;
    }

    public function name() : string
    {
        return $this->name;
    }
}
