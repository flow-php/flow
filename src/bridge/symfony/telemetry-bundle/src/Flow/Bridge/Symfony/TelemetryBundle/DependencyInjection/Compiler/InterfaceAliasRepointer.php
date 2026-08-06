<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Symfony\Component\DependencyInjection\Alias;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function interface_exists;
use function is_a;

/**
 * Decorating a service silently retargets every interface alias pointing at it. When the decorator does not
 * implement one of those interfaces, CheckAliasValidityPass (framework-bundle >= 7.3) fails the build - e.g.
 * FrameworkBundle aliases NamespacedPoolInterface to cache.app, which our cache decorator cannot satisfy.
 *
 * Such an alias is pointed back at the undecorated inner service, so the alias keeps resolving to a class that
 * honours its contract. Consumers type-hinting that interface get the untraced pool; every other consumer -
 * by service id, or by an interface the decorator does implement - still gets the traced one.
 */
final readonly class InterfaceAliasRepointer
{
    public function __construct(
        private ContainerBuilder $container,
    ) {}

    /**
     * @param class-string $decoratorClass
     */
    public function repoint(string $decoratedServiceId, string $decoratorId, string $decoratorClass): void
    {
        foreach ($this->container->getAliases() as $aliasId => $alias) {
            if ((string) $alias !== $decoratedServiceId) {
                continue;
            }

            if (!interface_exists($aliasId)) {
                continue;
            }

            if (is_a($decoratorClass, $aliasId, true)) {
                continue;
            }

            $innerId = $decoratorId . '.inner';
            $innerAlias = new Alias($innerId, $alias->isPublic());

            if ($alias->isDeprecated()) {
                // getDeprecation() substitutes the id into the template; asking for the placeholder itself
                // returns the template unchanged, which is what setDeprecated() requires back.
                /** @var array{package: string, version: string, message: string} $deprecation */
                $deprecation = $alias->getDeprecation('%alias_id%');
                $innerAlias->setDeprecated($deprecation['package'], $deprecation['version'], $deprecation['message']);
            }

            $this->container->setAlias($aliasId, $innerAlias);
        }
    }
}
