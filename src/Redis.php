<?php


namespace rabbit\db\redis;

use rabbit\db\ConnectionInterface;
use rabbit\redis\SwooleRedis;

/**
 * Class Redis
 * @package rabbit\db\redis
 */
class Redis extends SwooleRedis implements ConnectionInterface
{
}
