<?php declare(strict_types=1);

namespace Flow\RDSL;

interface Executable
{
    public function addMethodCall(Method $executable) : self;

    public function arguments() : Arguments;

    public function call() : ?Method;

    public function name() : string;
}
