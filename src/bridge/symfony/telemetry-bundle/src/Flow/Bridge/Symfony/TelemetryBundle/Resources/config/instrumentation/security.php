<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\SecuritySpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\UserAttributeResolver;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;
use function Symfony\Component\DependencyInjection\Loader\Configurator\tagged_iterator;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services->set('flow.telemetry.security.user_attribute_resolver', UserAttributeResolver::class)->args([
        tagged_iterator('flow.telemetry.security.user_attribute_provider'),
        '%flow.telemetry.security.field.id_attribute%',
        '%flow.telemetry.security.field.roles_attribute%',
        '%flow.telemetry.security.field.email_attribute%',
        '%flow.telemetry.security.field.email_getter%',
    ]);

    $services
        ->set('flow.telemetry.security.span_subscriber', SecuritySpanSubscriber::class)
        ->args([
            service('security.token_storage'),
            service('flow.telemetry.security.user_attribute_resolver'),
        ])
        ->tag('kernel.event_subscriber');
};
