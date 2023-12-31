<?php declare(strict_types=1);

namespace Flow\RDSL;

final class Executor
{
    public function __construct(
    ) {
    }

    /**
     * @param Executables $executables
     *
     * @return array<mixed>
     */
    public function execute(Executables $executables) : array
    {
        $results = [];

        foreach ($executables->toArray() as $executable) {
            $results[] = $this->executeFunction($executable);
        }

        return $results;
    }

    public function executeFunction(DSLFunction $executable) : mixed
    {
        /** @phpstan-ignore-next-line */
        $result = $executable->name()(...$this->prepareArguments($executable->arguments()));

        if ($executable->call()) {
            return $this->executeMethod($result, $executable->call());
        }

        return $result;
    }

    public function executeMethod(object $context, Method $executable) : mixed
    {
        $methodName = $executable->name();

        $result = $context->{$methodName}(...$this->prepareArguments($executable->arguments()));

        if ($executable->call()) {
            return $this->executeMethod($result, $executable->call());
        }

        return $result;
    }

    public function prepareArguments(Arguments $arguments) : array
    {
        $args = [];

        foreach ($arguments->toArray() as $name => $argument) {
            if ($argument instanceof DSLFunction) {
                $args[$name] = $this->executeFunction($argument);
            } else {
                $args[$name] = $argument;
            }
        }

        return $args;
    }
}
