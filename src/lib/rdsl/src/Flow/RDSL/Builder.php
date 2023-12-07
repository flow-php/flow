<?php declare(strict_types=1);

namespace Flow\RDSL;

use Flow\RDSL\Exception\InvalidArgumentException;

final class Builder
{
    public function __construct(
        private readonly Finder $finder
    ) {
    }

    public function parse(array $definition) : Executables
    {
        if (!\count($definition)) {
            throw new InvalidArgumentException('Definition must have at least one function: [{"function":"name","args":[]}]');
        }

        $executables = [];

        foreach ($definition as $function) {
            $executables[] = $this->parseFunction($function, true);
        }

        return new Executables($executables);
    }

    public function parseArg(mixed $definition) : mixed
    {
        if (\is_scalar($definition)) {
            return $definition;
        }

        if (!\is_array($definition)) {
            throw new InvalidArgumentException(\sprintf('Argument must be a scalar or an array, got "%s"', \gettype($definition)));
        }

        if (\array_key_exists('function', $definition)) {
            return $this->parseFunction($definition);
        }

        if (\array_key_exists('method', $definition)) {
            throw new InvalidArgumentException('Method are allowed only in calls, if you want to pass a method as argument, use a function instead');
        }

        return $this->parseArg($definition);
    }

    public function parseArgs(array $definition) : Arguments
    {
        $arguments = [];

        foreach ($definition as $name => $argDefinition) {
            if (\is_string($name)) {
                $arguments[$name] = $this->parseArg($argDefinition);
            } else {
                $arguments[] = $this->parseArg($argDefinition);
            }
        }

        return new Arguments($arguments);
    }

    public function parseFunction(array $definition, bool $entryPoint = false) : DSLFunction
    {
        if (!\array_key_exists('function', $definition)) {
            throw new InvalidArgumentException('Definition must start with a function: {"function":"name","args":[]}');
        }

        if (!\is_string($definition['function'])) {
            throw new InvalidArgumentException('Definition must start with a function: {"function":"name","args":[]}');
        }

        $args = $definition['args'] ?? [];

        if (\is_array($args) === false) {
            throw new InvalidArgumentException(\sprintf('Arguments definition must be an array, got "%s"', \gettype($args)));
        }

        $callDefinition = $definition['call'] ?? null;

        $functionReflection = $this->finder->findFunction($definition['function'], $entryPoint);

        if ($callDefinition === null) {
            return new DSLFunction(
                $functionReflection->name,
                new Arguments($args)
            );
        }

        $functionReturnType = $functionReflection->getReturnType();

        if (!$functionReturnType instanceof \ReflectionNamedType) {
            throw new InvalidArgumentException(\sprintf('Function "%s" must have return type', $functionReflection->name));
        }

        return (new DSLFunction($functionReflection->name, $this->parseArgs($args)))
            ->addMethodCall($this->parseMethod($functionReturnType, $callDefinition));
    }

    public function parseMethod(\ReflectionNamedType $object, array $definition) : Method
    {
        $objectClass = $this->finder->findClass($object->getName());

        if (!\array_key_exists('method', $definition)) {
            throw new InvalidArgumentException('Method definition must start with a method: {"method":"name","args":[]}');
        }

        if (!\is_string($definition['method'])) {
            throw new InvalidArgumentException('Method definition must start with a method: {"method":"name","args":[]}');
        }

        if (!$objectClass->hasMethod($definition['method'])) {
            throw new InvalidArgumentException(\sprintf('Method "%s" does not exists in class "%s"', $definition['method'], $objectClass->getName()));
        }

        $args = $definition['args'] ?? [];

        if (\is_array($args) === false) {
            throw new InvalidArgumentException(\sprintf('Arguments definition must be an array, got "%s"', \gettype($args)));
        }

        $callDefinition = $definition['call'] ?? null;

        $methodReflection = $objectClass->getMethod($definition['method']);

        if ($callDefinition === null) {
            return new Method($methodReflection->name, $this->parseArgs($args));
        }

        $returnType = $methodReflection->getReturnType();

        if (!$returnType instanceof \ReflectionNamedType) {
            throw new InvalidArgumentException(\sprintf('Method "%s::%s" must have return type', $objectClass->getName(), $methodReflection->name));
        }

        return (new Method($methodReflection->name, new Arguments($args)))
            ->addMethodCall($this->parseMethod($returnType, $callDefinition));
    }
}
