<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

/**
 * What a messenger span uses for the part of its name after the operation ("send" / "process").
 */
enum MessageNaming: string
{
    /**
     * The transport, e.g. "process async"
     */
    case Transport = 'transport';

    /**
     * The message short class name, e.g. "send CreateOrderMessage".
     */
    case MessageName = 'message_name';

    /**
     * The message fully qualified class name, e.g. "send App\Message\CreateOrderMessage".
     */
    case MessageFqcn = 'message_fqcn';
}
