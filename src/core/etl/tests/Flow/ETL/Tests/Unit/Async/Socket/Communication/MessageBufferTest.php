<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Async\Socket\Communication;

use Flow\ETL\Async\Socket\Communication\Message;
use Flow\ETL\Async\Socket\Communication\MessageBuffer;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

final class MessageBufferTest extends TestCase
{
    public function test_returning_long_message() : void
    {
        $message = Message::process(
            new Rows(
                ...\array_map(
                    function ($i) {
                        return Row::create(
                            Entry::integer('id', $i),
                            Entry::string('name', 'test'),
                            Entry::boolean('active', true)
                        );
                    },
                    \range(0, 10)
                )
            )
        );
        $buffer = new MessageBuffer($serializer = new CompressingSerializer());

        // total length 422
        $messageParts = \str_split($message->toString($serializer), 100);

        $this->assertNull($buffer->buffer($connection = new \stdClass(), $messageParts[0]));
        $this->assertNull($buffer->buffer($connection, $messageParts[1]));
        $this->assertNull($buffer->buffer($connection, $messageParts[2]));
        $this->assertNull($buffer->buffer($connection, $messageParts[3]));

        $this->assertEquals(
            $message,
            $buffer->buffer($connection, $messageParts[4])
        );
    }

    public function test_returning_short_message() : void
    {
        $message = Message::identify('id');

        $buffer = new MessageBuffer($serializer = new NativePHPSerializer());

        $this->assertEquals(
            $message,
            $buffer->buffer(new \stdClass(), $message->toString($serializer))
        );
    }
}
