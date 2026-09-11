<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use PhpParser\Node;
use PhpParser\Node\Expr\FuncCall;
use PhpParser\Node\Expr\MethodCall;
use PhpParser\Node\Expr\New_;
use PhpParser\Node\Expr\NullsafeMethodCall;
use PhpParser\Node\Expr\StaticCall;
use PhpParser\Node\Expr\Variable;
use PhpParser\Node\Identifier;
use PhpParser\Node\Name;
use ReflectionFunction;
use ReflectionMethod;
use ReflectionNamedType;
use Throwable;

use function array_key_exists;
use function class_exists;
use function function_exists;
use function interface_exists;
use function is_string;

/**
 * Scope is one .md file, in fence order, because pages routinely assign a variable in one block and
 * use it several blocks later. Nothing is guessed: a variable is typed only when a declaration says
 * what it is, so an unresolvable receiver yields no verdict rather than a wrong one.
 */
final class InferredTypes
{
    /** @var array<string, class-string> */
    private array $inherited = [];

    /** @var array<string, class-string> */
    private array $local = [];

    /**
     * Everything typed so far becomes background knowledge: still good enough to continue a chain,
     * no longer good enough to convict a call, because a later block may reuse the name for
     * something else entirely.
     */
    public function beginFence(): void
    {
        $this->inherited = [...$this->inherited, ...$this->local];
        $this->local = [];
    }

    public function remember(Variable $variable, Node $value): void
    {
        $name = $variable->name;
        $type = $this->of($value);

        if (is_string($name) && $type !== null) {
            $this->local[$name] = $type;
        }
    }

    /**
     * Whether this fence is what established the receiver of the call, walking to the root of the
     * chain. A call whose receiver was typed by an earlier block is not judged: the fence in front
     * of the reader does not say what that variable holds.
     */
    public function establishedHere(Node $node): bool
    {
        while ($node instanceof MethodCall || $node instanceof NullsafeMethodCall || $node instanceof StaticCall) {
            if ($node instanceof StaticCall) {
                return true;
            }

            $node = $node->var;
        }

        if ($node instanceof New_ || $node instanceof FuncCall) {
            return true;
        }

        return $node instanceof Variable && is_string($node->name) && array_key_exists($node->name, $this->local);
    }

    /**
     * @return null|class-string
     */
    public function of(Node $node): ?string
    {
        if ($node instanceof Variable && is_string($node->name)) {
            return $this->local[$node->name] ?? $this->inherited[$node->name] ?? null;
        }

        if ($node instanceof New_ && $node->class instanceof Name) {
            return $this->classOf($node->class->toString());
        }

        if ($node instanceof FuncCall && $node->name instanceof Name) {
            return $this->returnTypeOfFunction($node->name->toString());
        }

        if (($node instanceof MethodCall || $node instanceof NullsafeMethodCall) && $node->name instanceof Identifier) {
            $receiver = $this->of($node->var);

            return $receiver === null ? null : $this->returnTypeOfMethod($receiver, $node->name->toString());
        }

        if ($node instanceof StaticCall && $node->class instanceof Name && $node->name instanceof Identifier) {
            $receiver = $this->classOf($node->class->toString());

            return $receiver === null ? null : $this->returnTypeOfMethod($receiver, $node->name->toString());
        }

        return null;
    }

    /**
     * @return null|class-string
     */
    private function classOf(string $name): ?string
    {
        return class_exists($name) || interface_exists($name) ? $name : null;
    }

    /**
     * @return null|class-string
     */
    private function returnTypeOfFunction(string $name): ?string
    {
        if (!function_exists($name)) {
            return null;
        }

        return $this->named((new ReflectionFunction($name))->getReturnType());
    }

    /**
     * @param class-string $class
     *
     * @return null|class-string
     */
    private function returnTypeOfMethod(string $class, string $method): ?string
    {
        try {
            $reflection = new ReflectionMethod($class, $method);
        } catch (Throwable) {
            return null;
        }

        $returns = $this->named($reflection->getReturnType());

        return match ($returns) {
            'static' => $class,
            'self' => $reflection->getDeclaringClass()->getName(),
            default => $returns,
        };
    }

    /**
     * @return null|class-string|"self"|"static"
     */
    private function named(mixed $type): ?string
    {
        if (!$type instanceof ReflectionNamedType) {
            return null;
        }

        $name = $type->getName();

        if ($name === 'static' || $name === 'self') {
            return $name;
        }

        return $this->classOf($name);
    }
}
