<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

/**
 * What a routed request span uses for its name and the http.route attribute.
 */
enum RouteNaming: string
{
    case Path = 'path';

    /**
     * The Symfony route name, e.g. order_show.
     */
    case Name = 'name';
}
