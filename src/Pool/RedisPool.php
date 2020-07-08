<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis\Pool;

use Rabbit\DB\Redis\Connection;
use Rabbit\Pool\ConnectionPool;

/**
 * Class RedisPool
 * @package Rabbit\DB\Redis\Pool
 */
class RedisPool extends ConnectionPool
{
    /**
     * @var string
     */
    protected string $connection = Connection::class;

    /**
     * @return mixed
     */
    public function create()
    {
        return new $this->connection($this->getPoolConfig()->getName());
    }
}
