<?php

declare(strict_types=1);

if (!function_exists('dd')) {
    function dd(...$args): void
    {
        $header = '';

        try {
            throw new RuntimeException();
        } catch (Exception $e) {
            $trace = $e->getTrace();

            if (isset($trace[0]) && is_array($trace[0])) {
                $header = (string) ($trace[0]['file'] ?? '') . ':' . (string) ($trace[0]['line'] ?? '');
            }
        }

        print PHP_EOL . $header . PHP_EOL;

        // @mago-expect analysis:mixed-assignment
        foreach ($args as $arg) {
            // @mago-expect lint:disallowed-functions
            var_dump($arg);
        }

        exit(1);
    }
}

if (!function_exists('dj')) {
    function dj(mixed $args, int $indention = 0, ?string $header = null): void
    {
        if (!is_array($args)) {
            $args = [$args];
        }

        if ($header === null) {
            $header = '';

            try {
                throw new RuntimeException();
            } catch (Exception $e) {
                $trace = $e->getTrace();

                if (isset($trace[0]) && is_array($trace[0])) {
                    $header = (string) ($trace[0]['file'] ?? '') . ':' . (string) ($trace[0]['line'] ?? '');
                }
            }
        }

        print PHP_EOL . str_repeat(' ', $indention) . $header . PHP_EOL;
        print str_repeat(' ', $indention) . '[' . PHP_EOL;

        if (array_is_list($args)) {
            // @mago-expect analysis:mixed-assignment
            foreach ($args as $v) {
                if (is_object($v)) {
                    if (method_exists($v, '__debugInfo')) {
                        print str_repeat(' ', $indention + 2) . (string) json_encode($v->__debugInfo()) . PHP_EOL;

                        continue;
                    }

                    if (method_exists($v, '__toString')) {
                        print str_repeat(' ', $indention + 2) . (string) json_encode($v->__toString()) . PHP_EOL;

                        continue;
                    }
                }

                print str_repeat(' ', $indention + 2) . (string) json_encode($v) . PHP_EOL;
            }

            print PHP_EOL;

            return;
        }

        // @mago-expect analysis:mixed-assignment
        foreach ($args as $i => $v) {
            if (is_object($v)) {
                if (method_exists($v, '__debugInfo')) {
                    print
                        str_repeat(' ', $indention + 2) . $i . ': ' . (string) json_encode($v->__debugInfo()) . PHP_EOL;

                    continue;
                }

                if (method_exists($v, '__toString')) {
                    print
                        str_repeat(' ', $indention + 2) . $i . ': ' . (string) json_encode($v->__toString()) . PHP_EOL;

                    continue;
                }
            }

            print str_repeat(' ', $indention + 2) . $i . ': ' . (string) json_encode($v) . PHP_EOL;
        }

        print str_repeat(' ', $indention) . ']' . PHP_EOL;
    }
}

if (!function_exists('ddj')) {
    function ddj(mixed $args, int $indention = 0): void
    {
        $header = '';

        try {
            throw new RuntimeException();
        } catch (Exception $e) {
            $trace = $e->getTrace();

            if (isset($trace[0]) && is_array($trace[0])) {
                $header = (string) ($trace[0]['file'] ?? '') . ':' . (string) ($trace[0]['line'] ?? '');
            }
        }

        if (!is_array($args)) {
            $args = [$args];
        }

        // @mago-expect lint:disallowed-functions
        dj($args, $indention, $header);

        exit(1);
    }
}
