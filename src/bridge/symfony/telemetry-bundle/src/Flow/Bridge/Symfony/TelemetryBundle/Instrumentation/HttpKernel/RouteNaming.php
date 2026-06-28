<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

/**
 * What a routed request span uses for its name and the http.route attribute.
 */
enum RouteNaming: string
{
    /**
     * The route path template, e.g. /orders/{id} — the OpenTelemetry HTTP semantic-conventions value
     * for http.route. Requires the router to resolve the template from the route name.
     */
    case Path = 'path';

    /**
     * The Symfony route name, e.g. order_show.
     */
    case Name = 'name';
}
