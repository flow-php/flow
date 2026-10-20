<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use Closure;

use function count;
use function get_class;
use function is_object;
use function is_string;

final readonly class ControllerName
{
    public function __construct(
        public string $name,
        public ?string $namespace = null,
        public ?string $function = null,
    ) {}

    /**
     * @param array<int, object|string>|callable|object $controller
     */
    public static function resolve(callable|object|array $controller): ?self
    {
        if (is_array($controller)) {
            if (count($controller) === 2) {
                $firstElement = $controller[0];
                $secondElement = $controller[1];
                $class = is_object($firstElement) ? $firstElement::class : $firstElement;
                $method = is_string($secondElement) ? $secondElement : '';

                return new self("{$class}::{$method}", $class, $method);
            }

            return null;
        }

        if (is_object($controller)) {
            if ($controller instanceof Closure) {
                return new self('Closure');
            }

            $class = get_class($controller);

            return new self($class . '::__invoke', $class, '__invoke');
        }

        if (is_string($controller)) {
            return new self($controller);
        }

        return null;
    }
}
