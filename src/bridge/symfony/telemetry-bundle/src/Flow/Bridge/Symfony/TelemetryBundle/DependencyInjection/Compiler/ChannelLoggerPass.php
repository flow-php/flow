<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\FlowTelemetryBundle;
use Flow\Telemetry\Logger\Logger;
use Psr\Log\LoggerInterface;
use Symfony\Component\DependencyInjection\Argument\BoundArgument;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

use function is_array;
use function is_string;
use function lcfirst;
use function sprintf;
use function str_replace;
use function ucwords;

/**
 * Routes services to a named telemetry logger based on their logging channel.
 *
 * Two populations are handled:
 *
 *  - Services explicitly opting in via the "flow.telemetry.channel" tag
 *    (typically through the #[WithTelemetryChannel] attribute). A missing or
 *    empty channel here is a configuration error and throws.
 *  - When "flow.telemetry.capture_framework_channels" is enabled, services
 *    Symfony tags "monolog.logger" (router, request, http_client, cache,
 *    messenger, …). This makes the bundle a drop-in for MonologBundle's channel
 *    routing without Monolog. We do not own that tag, so a tag without a usable
 *    channel is skipped rather than treated as an error.
 *
 * For each routed service the channel's PSR-3 logger is bound to its autowired
 * LoggerInterface argument and the native Flow Logger to its autowired Logger
 * argument; every explicit "logger" reference (in constructor arguments and method
 * calls) is rewritten to the PSR-3 logger, preserving the reference's
 * invalid-behavior flag. Per channel, both a "LoggerInterface $<channel>Logger" and
 * a "Logger $<channel>Logger" autowiring alias are registered so any service can
 * request a channel logger by named argument. Each channel is synthesized on demand
 * carrying a "flow.log.channel" attribute; where it is placed - the instrumentation scope,
 * every emitted record, or both - is governed by the
 * "flow.telemetry.channel_attribute_target" parameter (default "both"). A logger
 * already declared under that name (e.g. the always-present "default") is reused as-is.
 */
final class ChannelLoggerPass implements CompilerPassInterface
{
    /**
     * log.* is a reserved OTel namespace, so the flow-custom channel key carries the flow prefix.
     */
    public const string ATTR_LOG_CHANNEL = 'flow.log.channel';

    public const string TAG = 'flow.telemetry.channel';

    private const string CAPTURE_PARAMETER = 'flow.telemetry.capture_framework_channels';

    private const string ENABLED_PARAMETER = 'flow.telemetry.enabled';

    private const string ATTRIBUTE_TARGET_PARAMETER = 'flow.telemetry.channel_attribute_target';

    private const string FRAMEWORK_TAG = 'monolog.logger';

    public function process(ContainerBuilder $container): void
    {
        /** @var array<string, array{0: string, 1: string}> $channels */
        $channels = [];

        if ($this->capturesFrameworkChannels($container)) {
            foreach ($container->findTaggedServiceIds(self::FRAMEWORK_TAG) as $serviceId => $tags) {
                // @mago-expect analysis:mixed-assignment
                foreach ($tags as $tag) {
                    $channel = $this->resolveChannel($tag, $container);

                    if ($channel === null) {
                        continue;
                    }

                    $channels[$channel] = $this->route($serviceId, $channel, $container);
                }
            }
        }

        foreach ($container->findTaggedServiceIds(self::TAG) as $serviceId => $tags) {
            // @mago-expect analysis:mixed-assignment
            foreach ($tags as $tag) {
                $channel = $this->requireChannel($serviceId, $tag, $container);

                $channels[$channel] = $this->route($serviceId, $channel, $container);
            }
        }

        foreach ($channels as $channel => [$nativeId, $psr3Id]) {
            $argument = $this->camelize($channel) . 'Logger';
            $container->registerAliasForArgument($psr3Id, LoggerInterface::class, $argument);
            $container->registerAliasForArgument($nativeId, Logger::class, $argument);
        }
    }

    private function bindLogger(Definition $definition, string $nativeId, string $psr3Id): void
    {
        $this->rewriteLoggerReferences($definition, $psr3Id);

        $bindings = $definition->getBindings();
        $bindings[LoggerInterface::class] = new BoundArgument(
            new Reference($psr3Id),
            false,
            BoundArgument::SERVICE_BINDING,
        );
        $bindings[Logger::class] = new BoundArgument(new Reference($nativeId), false, BoundArgument::SERVICE_BINDING);
        $definition->setBindings($bindings);
    }

    private function camelize(string $channel): string
    {
        return lcfirst(str_replace(' ', '', ucwords(str_replace(['_', '.', '-'], ' ', $channel))));
    }

    private function capturesFrameworkChannels(ContainerBuilder $container): bool
    {
        if (
            $container->hasParameter(self::ENABLED_PARAMETER)
            && $container->getParameter(self::ENABLED_PARAMETER) !== true
        ) {
            return false;
        }

        if (!$container->hasParameter(self::CAPTURE_PARAMETER)) {
            return false;
        }

        return $container->getParameter(self::CAPTURE_PARAMETER) === true;
    }

    /**
     * @return array{0: string, 1: string} the native logger id and its PSR-3 wrapper id
     */
    private function channelLoggerIds(string $channel, ContainerBuilder $container): array
    {
        $psr3Id = FlowTelemetryBundle::defineLogger(
            $channel,
            ['attributes' => $this->channelAttributes($channel, $container)],
            $container,
        );

        return ['flow.telemetry.' . $channel . '.logger', $psr3Id];
    }

    /**
     * @return array{scope?: array<string, string>, signal?: array<string, string>}
     */
    private function channelAttributes(string $channel, ContainerBuilder $container): array
    {
        $value = [self::ATTR_LOG_CHANNEL => $channel];

        return match ($this->channelAttributeTarget($container)) {
            'scope' => ['scope' => $value],
            'signal' => ['signal' => $value],
            default => ['scope' => $value, 'signal' => $value],
        };
    }

    private function channelAttributeTarget(ContainerBuilder $container): string
    {
        if (!$container->hasParameter(self::ATTRIBUTE_TARGET_PARAMETER)) {
            return 'both';
        }

        $target = $container->getParameter(self::ATTRIBUTE_TARGET_PARAMETER);

        return is_string($target) ? $target : 'both';
    }

    private function requireChannel(string $serviceId, mixed $tag, ContainerBuilder $container): string
    {
        // @mago-expect analysis:mixed-assignment
        $channel = is_array($tag) ? $tag['channel'] ?? null : null;

        if (!is_string($channel) || $channel === '') {
            throw new RuntimeException(sprintf(
                'Service "%s" is tagged "%s" but is missing the required non-empty "channel" attribute.',
                $serviceId,
                self::TAG,
            ));
        }

        // @mago-expect analysis:mixed-assignment
        $resolved = $container->getParameterBag()->resolveValue($channel);

        if (!is_string($resolved) || $resolved === '') {
            throw new RuntimeException(sprintf(
                'Channel "%s" on service "%s" did not resolve to a non-empty string.',
                $channel,
                $serviceId,
            ));
        }

        return $resolved;
    }

    private function resolveChannel(mixed $tag, ContainerBuilder $container): ?string
    {
        // @mago-expect analysis:mixed-assignment
        $channel = is_array($tag) ? $tag['channel'] ?? null : null;

        if (!is_string($channel) || $channel === '') {
            return null;
        }

        // @mago-expect analysis:mixed-assignment
        $resolved = $container->getParameterBag()->resolveValue($channel);

        if (!is_string($resolved) || $resolved === '') {
            return null;
        }

        return $resolved;
    }

    private function rewriteLoggerReferences(Definition $definition, string $psr3Id): void
    {
        $definition->setArguments($this->swapLoggerReferences($definition->getArguments(), $psr3Id));

        $methodCalls = $definition->getMethodCalls();

        // @mago-expect analysis:mixed-assignment
        foreach ($methodCalls as $index => $call) {
            if (!is_array($call) || !is_array($call[1] ?? null)) {
                continue;
            }

            /** @var array{0: string, 1: array<array-key, mixed>} $call */
            $call[1] = $this->swapLoggerReferences($call[1], $psr3Id);
            $methodCalls[$index] = $call;
        }

        $definition->setMethodCalls($methodCalls);
    }

    /**
     * @return array{0: string, 1: string} the native logger id and its PSR-3 wrapper id
     */
    private function route(string $serviceId, string $channel, ContainerBuilder $container): array
    {
        [$nativeId, $psr3Id] = $this->channelLoggerIds($channel, $container);
        $this->bindLogger($container->getDefinition($serviceId), $nativeId, $psr3Id);

        return [$nativeId, $psr3Id];
    }

    /**
     * @param array<mixed> $arguments
     *
     * @return array<mixed>
     */
    private function swapLoggerReferences(array $arguments, string $psr3Id): array
    {
        // @mago-expect analysis:mixed-assignment
        foreach ($arguments as $key => $argument) {
            if ($argument instanceof Reference && (string) $argument === 'logger') {
                $arguments[$key] = new Reference($psr3Id, $argument->getInvalidBehavior());
            }
        }

        return $arguments;
    }
}
